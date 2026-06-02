package org.sunbird.job.programaggregate.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.programaggregate.domain.CollectionProgress

import java.util

/**
 * Config for program-activity-aggregate-updater-v2.
 *
 * Key differences from v1:
 *  - Job name is "program-activity-aggregate-updater-v2"
 *  - contentReadURL has no trailing slash (prevents double-slash in URL)
 *  - windowShards removed (no count-window in v2 topology)
 *  - courseCacheExpiry / courseInMemoryCacheExpiry added for content cache TTL
 */
class ProgramActivityAggregateUpdaterConfigV2(override val config: Config)
  extends BaseJobConfig(config, "program-activity-aggregate-updater-v2") {

  private val serialVersionUID: Long = -987654321098765432L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val scalaMapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]] = TypeExtractor.getForClass(classOf[List[CollectionProgress]])

  // ── Kafka Topics ────────────────────────────────────────────────────────────
  val kafkaInputTopic: String    = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")
  val kafkaFailedEventTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaCertIssueTopic: String  = config.getString("kafka.output.certissue.topic")

  // ── Parallelism ─────────────────────────────────────────────────────────────
  override val kafkaConsumerParallelism: Int     = config.getInt("task.consumer.parallelism")
  val activityAggregateUpdaterParallelism: Int   = config.getInt("task.activity.agg.parallelism")
  val deDupProcessParallelism: Int               = config.getInt("task.dedup.parallelism")
  val enrolmentCompleteParallelism: Int          = config.getInt("task.enrolment.complete.parallelism")

  // ── Metrics ─────────────────────────────────────────────────────────────────
  val totalEventCount           = "total-events-count"
  val failedEventCount          = "failed-events-count"
  val dbUpdateCount             = "db-update-count"
  val dbReadCount               = "db-read-count"
  val cacheHitCount             = "cache-hit-count"
  val cacheMissCount            = "cache-miss-count"
  val batchEnrolmentUpdateEventCount = "batch-enrolment-update-count"
  val skipEventsCount           = "skipped-events-count"
  val processedEnrolmentCount   = "processed-enrolment-count"
  val enrolmentCompleteCount    = "enrolment-complete-count"
  val certIssueEventsCount      = "cert-issue-events-count"
  val retiredCCEventsCount      = "retired-consumption-events-count"

  // ── Cassandra ───────────────────────────────────────────────────────────────
  val dbUserContentConsumptionTable: String = config.getString("lms-cassandra.consumption.table")
  val dbUserActivityAggTable: String        = config.getString("lms-cassandra.user_activity_agg.table")
  val dbUserEnrolmentsTable: String         = config.getString("lms-cassandra.user_enrolments.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String     = config.getString("lms-cassandra.host")
  val dbPort: Int        = config.getInt("lms-cassandra.port")

  // ── Redis ───────────────────────────────────────────────────────────────────
  val nodeStore: Int         = config.getInt("redis.database.relationCache.id")
  val contentStoreIndex: Int = if (config.hasPath("redis.database.contentCache.id")) config.getInt("redis.database.contentCache.id") else 0
  val deDupRedisHost: String = config.getString("dedup-redis.host")
  val deDupRedisPort: Int    = config.getInt("dedup-redis.port")
  val deDupStore: Int        = config.getInt("dedup-redis.database.index")
  val deDupExpirySec: Int    = config.getInt("dedup-redis.database.expiry")

  // Redis cache TTL for course info (ms)
  val courseCacheExpiry: Int        = if (config.hasPath("activity.course.cache.expiry")) config.getInt("activity.course.cache.expiry") else 3600000
  val courseInMemoryCacheExpiry: Int = if (config.hasPath("activity.course.in.memory.cache.expiry")) config.getInt("activity.course.in.memory.cache.expiry") else 3600000

  // ── Output Tags ─────────────────────────────────────────────────────────────
  val uniqueConsumptionOutputTagName = "program-unique-consumption-events"
  val uniqueConsumptionOutput: OutputTag[Map[String, AnyRef]] = OutputTag[Map[String, AnyRef]](uniqueConsumptionOutputTagName)
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)
  val certIssueOutputTagName = "program-certificate-issue-events"
  val certIssueOutputTag: OutputTag[String] = OutputTag[String](certIssueOutputTagName)

  // ── Field-name Constants ─────────────────────────────────────────────────────
  val activityType     = "activity_type"
  val activityId       = "activity_id"
  val contextId        = "context_id"
  val activityUser     = "user_id"
  val aggLastUpdated   = "agg_last_updated"
  val agg              = "agg"
  val courseId         = "courseId"
  val batchId          = "batchId"
  val contentId        = "contentId"
  val progress         = "progress"
  val contents         = "contents"
  val contentStatus    = "contentStatus"
  val userId           = "userId"
  val status           = "status"
  val unitActivityType = "course-unit"
  val courseActivityType = "course"
  val leafNodes        = "leafnodes"
  val ancestors        = "ancestors"
  val viewcount        = "viewcount"
  val completedcount   = "completedcount"
  val complete         = "complete"
  val eData            = "edata"
  val action           = "action"
  val batchEnrolmentUpdateCode = "batch-enrolment-update"
  val aggregates       = "aggregates"
  val userid           = "userid"
  val courseid         = "courseid"
  val batchid          = "batchid"
  val active           = "active"
  val contentstatus    = "contentstatus"
  val leafNodesKey     = "leafNodes"

  // ── Operator Names ───────────────────────────────────────────────────────────
  val consumptionDeDupFn               = "program-consumption-dedup-process"
  val programactivityAggregateUpdaterFn = "program-activity-aggregate-updater-fn"
  val programActivityAggregateUpdaterConsumer = "program-activity-aggregate-updater-consumer"
  val programactivityAggregateUpdaterProducer = "program-activity-aggregate-updater-audit-events-sink"
  val programactivityAggFailedEventProducer   = "program-activity-aggregate-updater-failed-sink"
  val certIssueEventProducer = "certificate-issue-event-producer"
  val routerFn    = "RouterFn"
  val partition   = "partition"
  val courseBatch = "CourseBatch"

  // ── Thresholds (kept for reference / backward-compat) ───────────────────────
  val thresholdBatchReadInterval: Int = config.getInt("threshold.batch.read.interval")
  val thresholdBatchReadSize: Int     = config.getInt("threshold.batch.read.size")
  val thresholdBatchWriteSize: Int    = config.getInt("threshold.batch.write.size")

  // ── Feature Flags ───────────────────────────────────────────────────────────
  val moduleAggEnabled: Boolean           = config.getBoolean("activity.module.aggs.enabled")
  val dedupEnabled: Boolean               = config.getBoolean("activity.input.dedup.enabled")
  val statusCacheExpirySec: Int           = config.getInt("activity.collection.status.cache.expiry")
  val filterCompletedEnrolments: Boolean  = if (config.hasPath("activity.filter.processed.enrolments")) config.getBoolean("activity.filter.processed.enrolments") else true

  // ── External Services ────────────────────────────────────────────────────────
  val searchServiceBasePath: String = config.getString("service.search.basePath")
  val searchAPIURL                  = searchServiceBasePath + "/v3/search"
  val contentServiceBase: String    = config.getString("service.content.basePath")

  // No trailing slash — ContentHelper builds:  contentReadURL + "/" + courseId
  val contentReadURL = contentServiceBase + "/content/v3/read"

  val contentReadFields: String = if (config.hasPath("content.read.fields"))
    config.getString("content.read.fields")
  else
    "identifier,name,versionKey,parentCollections,primaryCategory,courseCategory,languageMapV1,leafNodes,language,milestones_v1,preliminaryAssessment"

  // ── Content / Course Metadata Keys ──────────────────────────────────────────
  val name: String               = "name"
  val identifier: String         = "identifier"
  val primaryCategory: String    = "primaryCategory"
  val versionKey: String         = "versionKey"
  val course: String             = "Course"
  val parentCollections: String  = "parentCollections"
  val skippedEventCount          = "skipped-event-count"
  val defaultHeaders             = Map[String, String]("Content-Type" -> "application/json")
  val userAccBlockedErrCode      = "UOS_USRRED0006"
  val courseCategory             = "courseCategory"
  val coursecategory             = "coursecategory"
  val preliminaryAssessment      = "preliminaryAssessment"
  val preliminary_Assessment_Key = "preliminaryassessment"

  val validProgramPrimaryCategory: List[String] = List("Program", "Curated Program", "Blended Program")
}
