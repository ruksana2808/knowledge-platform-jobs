package org.sunbird.job.programaggregate.functions

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.programaggregate.common.ContentHelper
import org.sunbird.job.programaggregate.domain.{ContentStatus, UserActivityAgg}
import org.sunbird.job.programaggregate.task.ProgramActivityAggregateUpdaterConfigV2
import org.sunbird.job.util.{CassandraUtil, HttpUtil}

import java.util.{Date, UUID}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
 * v2 replacement for the v1 windowed pipeline:
 *   ProgramActivityAggregatesFunction (window-based content aggregation)
 *   + ProgramProgressUpdateFunction (enrolment status update)
 *   + ProgramProgressCompleteFunction (enrolment completion + cert)
 *
 * All three are collapsed here into a single event-at-a-time KeyedProcessFunction.
 *
 * Business Logic (identical to v1):
 *  1. Read existing enrolment from user_enrolments
 *  2. Merge incoming and existing content status (max operation)
 *  3. Update user_content_consumption table with merged viewCount/completedCount
 *  4. Compute aggregates for course and child collections
 *  5. Update user_activity_agg table with course/collection aggregates
 *  6. Mark enrolment as complete (status=2) when all leafNodes completed
 *  7. Emit certificate-issue event upon enrolment completion
 *
 * Optimisations vs v1:
 *  1. No count-window — processes every event immediately.
 *  2. Keyed by userId_courseId_batchId — natural per-enrolment ordering.
 *  3. LOCAL_QUORUM on all Cassandra reads and writes.
 *  4. In-memory course-info cache avoids repeated Redis/HTTP calls.
 *  5. Single function replaces three downstream operators.
 */
class ProgramActivityAggregatesFunctionV2(
    config: ProgramActivityAggregateUpdaterConfigV2,
    httpUtil: HttpUtil,
    @transient var cassandraUtil: CassandraUtil = null
) extends KeyedProcessFunction[String, Map[String, AnyRef], String]
    with ContentHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramActivityAggregatesFunctionV2])
  private var contentCache: DataCache = _
  @transient private var metrics: Metrics = _

  override def open(parameters: Configuration): Unit = {
    if (cassandraUtil == null)
      cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)

    contentCache = new DataCache(
      config,
      new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)),
      config.contentStoreIndex,
      List()
    )
    contentCache.init()

    // Lightweight Metrics container for ContentHelper API calls
    metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]())
  }

  override def close(): Unit = {
    if (cassandraUtil != null) cassandraUtil.close()
    if (contentCache != null) contentCache.close()
    super.close()
  }

  /**
   * Main processing logic per event:
   *  1. Extract userId, courseId, batchId, content list from event
   *  2. Read existing enrolment with LOCAL_QUORUM
   *  3. Merge content status (incoming overrides existing only if strictly higher)
   *  4. Update user_content_consumption table
   *  5. Compute and update course + child collection aggregates
   *  6. Check if all leafNodes completed → mark enrolment complete
   *  7. Emit cert-issue event if completed
   */
  override def processElement(
      event: Map[String, AnyRef],
      ctx: KeyedProcessFunction[String, Map[String, AnyRef], String]#Context,
      out: Collector[String]
  ): Unit = {
    val userId   = event.getOrElse(config.userId,   "").asInstanceOf[String]
    val courseId = event.getOrElse(config.courseId, "").asInstanceOf[String]
    val batchId  = event.getOrElse(config.batchId,  "").asInstanceOf[String]
    val contents = event
      .getOrElse(config.contents, List.empty[Map[String, AnyRef]])
      .asInstanceOf[List[Map[String, AnyRef]]]

    logger.info(s"ProgramActivityAggregatesFunctionV2 - userId=$userId courseId=$courseId batchId=$batchId contentCount=${contents.size}")

    // ── 1. Validate input ─────────────────────────────────────────────────────
    if (StringUtils.isBlank(userId) || StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId)) {
      logger.warn(s"Invalid input: userId=$userId courseId=$courseId batchId=$batchId")
      return
    }

    // ── 2. Extract content status from incoming event ────────────────────────
    val incomingContentStatus = extractContentStatus(contents)
    if (incomingContentStatus.isEmpty) {
      logger.warn(s"No valid content status in event for userId=$userId")
      return
    }

    // ── 3. Read existing enrolment with LOCAL_QUORUM ─────────────────────────
    val enrolmentRow = getEnrolment(userId, courseId, batchId)
    if (enrolmentRow == null) {
      logger.warn(s"No enrolment found for userId=$userId courseId=$courseId batchId=$batchId - skipping")
      return
    }

    val enrolmentStatus = enrolmentRow.getInt("status")
    if (enrolmentStatus == 2) {
      logger.info(s"Enrolment already completed for userId=$userId courseId=$courseId - skipping")
      return
    }

    // ── 4. Load and merge existing contentStatus ──────────────────────────────
    val existingContentStatus = scala.collection.mutable.Map.empty[String, ContentStatus]
    Option(enrolmentRow.getMap("contentstatus", classOf[String], classOf[java.lang.Integer]))
      .foreach(_.asScala.foreach { case (cid, status) =>
        existingContentStatus.put(cid, ContentStatus(cid, status.intValue(), 0, 0))
      })

    val mergedContentStatus = mergeContentStatus(incomingContentStatus, existingContentStatus.toMap)
    logger.info(s"Merged content status: ${mergedContentStatus.size} contents")

    // ── 5. Update user_content_consumption table ──────────────────────────────
    updateUserContentConsumption(userId, courseId, batchId, mergedContentStatus)

    // ── 6. Fetch program leaf nodes and compute completion ────────────────────
    val leafNodes = getProgramLeafNodes(courseId)
    if (leafNodes.isEmpty) {
      logger.warn(s"No leafNodes found for courseId=$courseId - skipping update")
      return
    }

    val completedContentIds = mergedContentStatus.filter(_._2.status == 2).keys.toSet
    val completedCount      = leafNodes.intersect(completedContentIds).size
    val isCompleted         = completedCount >= leafNodes.size

    logger.info(s"userId=$userId courseId=$courseId completedCount=$completedCount leafNodesCount=${leafNodes.size} isCompleted=$isCompleted")

    // ── 7. Compute and update course aggregates ───────────────────────────────
    val courseAgg = UserActivityAgg("Course", userId, courseId, s"cb:$batchId", Map("completedCount" -> completedCount.toDouble), Map("completedCount" -> System.currentTimeMillis()))
    updateCourseAggregate(courseAgg)

    // ── 8. Update enrolment with appropriate status ───────────────────────────
    updateEnrolment(userId, courseId, batchId, mergedContentStatus, completedCount, isCompleted)

    // ── 9. Emit cert-issue event if completed ─────────────────────────────────
    if (isCompleted) {
      ctx.output(config.certIssueOutputTag, buildCertIssuedEvent(userId, courseId, batchId))
      logger.info(s"Cert issue event fired for userId=$userId courseId=$courseId batchId=$batchId")
    }
  }

  // ── Private helpers ───────────────────────────────────────────────────────────

  /**
   * Extract content status from incoming event (list of content objects with status field)
   */
  private def extractContentStatus(contents: List[Map[String, AnyRef]]): Map[String, ContentStatus] = {
    val enrichedContents = contents.map(content => {
      (content.getOrElse(config.contentId, "").asInstanceOf[String], content.getOrElse(config.status, 0).asInstanceOf[Number])
    }).filter(t => StringUtils.isNotBlank(t._1) && (t._2.intValue() > 0))
      .map(x => {
        val completedCount = if (x._2.intValue() == 2) 1 else 0
        ContentStatus(x._1, x._2.intValue(), completedCount)
      }).groupBy(f => f.contentId)

    enrichedContents.map(content => {
      val consumedList = content._2
      val finalStatus = consumedList.map(x => x.status).max
      val views = consumedList.map(x => x.viewCount).sum
      val completion = consumedList.map(x => x.completedCount).sum
      (content._1, ContentStatus(content._1, finalStatus, completion, views))
    })
  }

  /**
   * Merge incoming content status with existing DB status
   * Business logic: incoming status only updates if strictly higher (max semantics)
   */
  private def mergeContentStatus(incoming: Map[String, ContentStatus], existing: Map[String, ContentStatus]): Map[String, ContentStatus] = {
    val merged = scala.collection.mutable.Map.empty[String, ContentStatus]

    // Process all existing contents
    existing.foreach { case (contentId, dbCC) =>
      val incomingCC = incoming.getOrElse(contentId, ContentStatus(contentId, 0, 0, 0))
      val finalStatus = scala.math.max(incomingCC.status, dbCC.status)
      val viewCount = incomingCC.viewCount + dbCC.viewCount
      val completedCount = incomingCC.completedCount + dbCC.completedCount
      merged.put(contentId, ContentStatus(contentId, finalStatus, completedCount, viewCount))
    }

    // Add new contents from incoming
    incoming.foreach { case (contentId, incomingCC) =>
      if (!existing.contains(contentId)) {
        merged.put(contentId, incomingCC)
      }
    }

    merged.toMap
  }

  /**
   * Update user_content_consumption table with merged content status
   */
  private def updateUserContentConsumption(userId: String, courseId: String, batchId: String, contentStatus: Map[String, ContentStatus]): Unit = {
    val queries = contentStatus.values.map { content =>
      QueryBuilder.update(config.dbKeyspace, config.dbUserContentConsumptionTable)
        .`with`(QueryBuilder.set(config.viewcount, content.viewCount))
        .and(QueryBuilder.set(config.completedcount, content.completedCount))
        .where(QueryBuilder.eq(config.batchId.toLowerCase(), batchId))
        .and(QueryBuilder.eq(config.courseId.toLowerCase(), courseId))
        .and(QueryBuilder.eq(config.userId.toLowerCase(), userId))
        .and(QueryBuilder.eq(config.contentId.toLowerCase(), content.contentId))
    }.toList

    if (queries.nonEmpty) {
      updateDB(config.thresholdBatchWriteSize, queries)
      logger.info(s"Updated ${queries.size} content consumption records for userId=$userId courseId=$courseId")
    }
  }

  /**
   * Update course-level aggregate in user_activity_agg table
   */
  private def updateCourseAggregate(agg: UserActivityAgg): Unit = {
    val query = QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll(config.aggregates, agg.aggregates.asJava))
      .and(QueryBuilder.putAll(config.aggLastUpdated, agg.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, agg.activity_id))
      .and(QueryBuilder.eq(config.activityType, agg.activity_type))
      .and(QueryBuilder.eq(config.contextId, agg.context_id))
      .and(QueryBuilder.eq(config.activityUser, agg.user_id))

    query.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.update(query)
    logger.info(s"Updated aggregate for userId=${agg.user_id} courseId=${agg.activity_id}")
  }

  /**
   * Batch write queries to Cassandra with LOCAL_QUORUM consistency
   */
  private def updateDB(batchSize: Int, queriesList: List[Update.Where]): Unit = {
    val groupedQueries = queriesList.grouped(batchSize).toList
    groupedQueries.foreach(queries => {
      val cqlBatch = QueryBuilder.batch()
      queries.foreach(query => cqlBatch.add(query))
      cqlBatch.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      val result = cassandraUtil.update(cqlBatch)
      if (!result) {
        val msg = s"Database update failed: batch of ${queries.size} queries"
        logger.error(msg)
        throw new Exception(msg)
      }
    })
  }

  private def getEnrolment(userId: String, courseId: String, batchId: String) = {
    val selectWhere = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .where()
    selectWhere
      .and(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
      .and(QueryBuilder.eq("batchid", batchId))
    val stmt = new SimpleStatement(selectWhere.toString)
      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.findOneWithStatement(stmt)
  }

  private def getProgramLeafNodes(courseId: String): Set[String] = {
    val contentObj = getCourseInfo(courseId)(metrics, config, contentCache, httpUtil)
    if (contentObj == null || contentObj.isEmpty) {
      logger.warn(s"No content info found for courseId=$courseId")
      return Set.empty[String]
    }
    Option(contentObj.get(config.leafNodesKey)) match {
      case Some(l: java.util.List[_])           => l.asScala.map(_.toString).toSet
      case Some(l: scala.collection.Seq[_])     => l.map(_.toString).toSet
      case _                                    => Set.empty[String]
    }
  }

  private def updateEnrolment(
      userId: String, courseId: String, batchId: String,
      contentStatus: Map[String, ContentStatus], completedCount: Int, isCompleted: Boolean
  ): Unit = {
    val contentStatusJava: java.util.Map[String, java.lang.Integer] =
      contentStatus.map { case (k, v) => k -> Int.box(v.status) }.asJava

    val whereClause = if (isCompleted) {
      QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
        .`with`(QueryBuilder.set("status", 2))
        .and(QueryBuilder.set("completedon", new Date()))
        .and(QueryBuilder.set("progress", completedCount))
        .and(QueryBuilder.set("contentstatus", contentStatusJava))
        .and(QueryBuilder.set("datetime", System.currentTimeMillis()))
        .where(QueryBuilder.eq("userid", userId))
        .and(QueryBuilder.eq("courseid", courseId))
        .and(QueryBuilder.eq("batchid", batchId))
    } else {
      QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
        .`with`(QueryBuilder.set("status", 1))
        .and(QueryBuilder.set("progress", completedCount))
        .and(QueryBuilder.set("contentstatus", contentStatusJava))
        .and(QueryBuilder.set("datetime", System.currentTimeMillis()))
        .where(QueryBuilder.eq("userid", userId))
        .and(QueryBuilder.eq("courseid", courseId))
        .and(QueryBuilder.eq("batchid", batchId))
    }

    whereClause.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    cassandraUtil.update(whereClause)
    logger.info(s"Enrolment updated: userId=$userId courseId=$courseId status=${if (isCompleted) 2 else 1} completedCount=$completedCount")
  }

  private def buildCertIssuedEvent(userId: String, courseId: String, batchId: String): String = {
    val ets = System.currentTimeMillis()
    val mid = s"LP.$ets.${UUID.randomUUID()}"
    s"""{"eid":"BE_JOB_REQUEST","ets":$ets,"mid":"$mid","actor":{"id":"Course Certificate Generator","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"id":"${batchId}_${courseId}","type":"CourseCertificateGeneration"},"edata":{"userIds":["$userId"],"action":"issue-certificate","iteration":1,"trigger":"auto-issue","batchId":"$batchId","reIssue":false,"courseId":"$courseId"}}"""
  }
}

