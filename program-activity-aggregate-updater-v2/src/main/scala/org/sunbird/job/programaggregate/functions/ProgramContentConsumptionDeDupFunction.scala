package org.sunbird.job.programaggregate.functions

import com.datastax.driver.core.{ConsistencyLevel, Row, SimpleStatement}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.programaggregate.common.{ContentHelper, DeDupHelper}
import org.sunbird.job.programaggregate.task.ProgramActivityAggregateUpdaterConfigV2
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * De-duplication and event routing stage (unchanged from v1 logic).
 *
 * Receives raw Kafka events, filters to batch-enrolment-update actions,
 * looks up program membership, and emits one Map per (userId, programId) pair
 * to the uniqueConsumptionOutput side-tag for downstream aggregation.
 *
 * Uses LOCAL_QUORUM on all Cassandra reads.
 */
class ProgramContentConsumptionDeDupFunction(
    config: ProgramActivityAggregateUpdaterConfigV2,
    httpUtil: HttpUtil,
    @transient var cassandraUtil: CassandraUtil = null
)(implicit val stringTypeInfo: TypeInformation[String])
    extends BaseProcessFunction[util.Map[String, AnyRef], String](config)
    with ContentHelper {

  val mapType: Type = new TypeToken[Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramContentConsumptionDeDupFunction])
  var deDupEngine: DeDupEngine = _
  private var contentCache: DataCache = _
  private var collectionStatusCache: TTLCache[String, String] = _
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    contentCache = new DataCache(
      config,
      new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)),
      config.contentStoreIndex,
      List()
    )
    contentCache.init()
    collectionStatusCache = TTLCache[String, String](Duration.apply(config.statusCacheExpirySec, TimeUnit.SECONDS))
    deDupEngine = new DeDupEngine(
      config,
      new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)),
      config.deDupStore,
      config.deDupExpirySec
    )
    deDupEngine.init()
  }

  override def close(): Unit = {
    if (cassandraUtil != null) cassandraUtil.close()
    if (contentCache != null) contentCache.close()
    deDupEngine.close()
    super.close()
  }

  override def processElement(
      event: util.Map[String, AnyRef],
      context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
      metrics: Metrics
  ): Unit = {
    metrics.incCounter(config.totalEventCount)
    val eData = event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala
    val isBatchEnrollmentEvent = StringUtils.equalsIgnoreCase(
      eData.getOrElse(config.action, "").asInstanceOf[String],
      config.batchEnrolmentUpdateCode
    )

    if (isBatchEnrollmentEvent) {
      val contents = eData
        .getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]())
        .asInstanceOf[util.List[java.util.Map[String, AnyRef]]]
        .asScala

      logger.info("Input Event: " + contents)

      // Only process events that have at least one completed (status=2) content
      val filteredContents = contents.filter { x =>
        Option(x.get("status")).exists {
          case n: Number => n.intValue() == 2
          case _         => false
        }
      }.map(_.asScala.toMap).toList

      if (filteredContents.nonEmpty) {
        val eventInfoMap: mutable.Iterable[Map[String, AnyRef]] =
          getProgramEvent(eData.toMap)(metrics, config, httpUtil, contentCache)
        logger.info("EventInfoMap: " + eventInfoMap)
        if (eventInfoMap.nonEmpty) {
          eventInfoMap.foreach(d => context.output(config.uniqueConsumptionOutput, d))
          logger.info("Emitted " + eventInfoMap.size + " program event(s)")
        }
      } else {
        metrics.incCounter(config.skipEventsCount)
      }
    } else {
      metrics.incCounter(config.skipEventsCount)
    }
  }

  override def metricsList(): List[String] =
    List(config.totalEventCount, config.skipEventsCount, config.batchEnrolmentUpdateEventCount, config.dbReadCount)

  /**
   * Routes the incoming content-consumption event to the correct program(s).
   *
   *  - If the course itself is a Program / Curated Program / Blended Program → emit once.
   *  - If it is a Course / Standalone Assessment that belongs to one or more Programs
   *    (via parentCollections) AND the user is enrolled in those programs → emit one
   *    event per enrolled parent program.
   */
  def getProgramEvent(eventData: Map[String, AnyRef])(
      metrics: Metrics,
      config: ProgramActivityAggregateUpdaterConfigV2,
      httpUtil: HttpUtil,
      contentCache: DataCache
  ): mutable.Iterable[Map[String, AnyRef]] = {
    logger.info("EventInfo: " + eventData)
    val eventInfoMap = mutable.ListBuffer.empty[Map[String, AnyRef]]

    val userId   = eventData.getOrElse(config.userId,   "").asInstanceOf[String]
    val courseId = eventData.getOrElse(config.courseId, "").asInstanceOf[String]

    val contentObj: java.util.Map[String, AnyRef] = getCourseInfo(courseId)(metrics, config, contentCache, httpUtil)
    val primaryCategory  = contentObj.get(config.primaryCategory).asInstanceOf[String]
    val parentCollections: List[String] = Option(contentObj.get(config.parentCollections))
      .collect {
        case list: java.util.List[_]                        => list.asInstanceOf[java.util.List[String]].asScala.toList
        case scalaList: scala.collection.immutable.List[_]  => scalaList.asInstanceOf[scala.collection.immutable.List[String]]
      }
      .getOrElse(List.empty)

    logger.info(s"primaryCategory=$primaryCategory parentCollections=$parentCollections")

    if (config.validProgramPrimaryCategory.contains(primaryCategory)) {
      // The course IS the program → emit directly
      val contentConsumption = eventData
        .getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]())
        .asInstanceOf[util.List[java.util.Map[String, AnyRef]]]
        .asScala.map(_.asScala.toMap).toList
      eventInfoMap += eventData.updated(config.contents, contentConsumption)

    } else if (
      ("Course".equalsIgnoreCase(primaryCategory) || "Standalone Assessment".equalsIgnoreCase(primaryCategory)) &&
      parentCollections.nonEmpty
    ) {
      // The course belongs to programs → look up user's program enrolments
      val userEnrolments = getAllEnrolments(userId)(metrics)
      for (parentId <- parentCollections) {
        val row = userEnrolments.getOrElse(parentId, null)
        if (row != null) {
          val contentConsumption = eventData
            .getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]())
            .asInstanceOf[util.List[java.util.Map[String, AnyRef]]]
            .asScala
          val filteredContents = contentConsumption.filter(_.get("status") == 2).map(_.asScala.toMap).toList
          if (filteredContents.nonEmpty) {
            val enrolledBatchId: String = row.get("batchId") match {
              case Some(v: String) => v
              case _ => null
            }
            if (enrolledBatchId != null) {
              val eventInfoProgram = Map[String, AnyRef](
                "contents"  -> filteredContents,
                "userId"    -> userId,
                "action"    -> "batch-enrolment-update",
                "iteration" -> 1.asInstanceOf[Integer],
                "batchId"   -> enrolledBatchId,
                "courseId"  -> parentId
              )
              eventInfoMap += eventInfoProgram
              logger.info("Emitting program event: " + eventInfoProgram)
            } else {
              logger.error(s"BatchId is null for userId=$userId courseId=$parentId")
            }
          }
        }
      }
    } else {
      logger.error(s"Not a valid primary category: $primaryCategory parentCollections=$parentCollections")
    }

    eventInfoMap
  }

  /** Reads ALL active enrolments for a user with LOCAL_QUORUM consistency. */
  def getAllEnrolments(userId: String)(implicit metrics: Metrics): Map[String, Map[String, AnyRef]] = {
    val selectWhere: Select.Where = QueryBuilder
      .select(config.userid, config.courseid, config.batchid, config.active)
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .where()
    selectWhere.and(QueryBuilder.eq(config.userid, userId))

    val stmt = new SimpleStatement(selectWhere.toString).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    metrics.incCounter(config.dbReadCount)
    val rows: util.List[Row] = cassandraUtil.findAllWithStatement(stmt)

    if (rows == null || rows.isEmpty) Map.empty
    else
      rows.asScala
        .filter(r => r != null && r.getString(config.userid) != null)
        .map { r =>
          val cId = r.getString(config.courseid)
          cId -> Map[String, AnyRef](
            "batchId" -> r.getString(config.batchid),
            "active"  -> Boolean.box(Option(r.getBool(config.active)).getOrElse(false))
          )
        }
        .toMap
  }
}

