package org.sunbird.job.programaggregate.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.programaggregate.functions.{ProgramActivityAggregatesFunctionV2, ProgramContentConsumptionDeDupFunction}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

/**
 * v2 Stream Topology
 * ──────────────────────────────────────────────────────────────────────────────
 *
 * Kafka source
 *   └─► DeDup (ProgramContentConsumptionDeDupFunction)
 *         │ side-output: uniqueConsumptionOutput (Map[String, AnyRef])
 *         └─► keyBy(userId_courseId_batchId)
 *               └─► ProgramActivityAggregatesFunctionV2  [KeyedProcessFunction]
 *                     │ side-output: certIssueOutputTag
 *                     └─► Kafka cert-issue topic
 *
 * v1 → v2 topology changes:
 *  - Removed countWindow(thresholdBatchReadSize): events are processed immediately.
 *  - Removed custom hash KeySelector: keyed by natural composite string.
 *  - Removed 3 downstream ProcessFunctions (EnrolUpdate, ProgressUpdate,
 *    ProgressComplete): all logic is now inline in ProgramActivityAggregatesFunctionV2.
 *  - Removed auditEventOutputTag / failedEventOutputTag sinks (not needed in v2).
 */
class ProgramActivityAggregateUpdaterStreamTaskV2(
    config: ProgramActivityAggregateUpdaterConfigV2,
    kafkaConnector: FlinkKafkaConnector,
    httpUtil: HttpUtil
) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
      TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val scalaMapTypeInfo: TypeInformation[Map[String, AnyRef]] =
      TypeExtractor.getForClass(classOf[Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] =
      TypeExtractor.getForClass(classOf[String])

    // ── Stage 1 : De-duplication & program-event routing ─────────────────────
    val dedupProcess = env
      .addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic))
      .name(config.programActivityAggregateUpdaterConsumer)
      .uid(config.programActivityAggregateUpdaterConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new ProgramContentConsumptionDeDupFunction(config, httpUtil))
      .name(config.consumptionDeDupFn)
      .uid(config.consumptionDeDupFn)
      .setParallelism(config.deDupProcessParallelism)

    // ── Stage 2 : Aggregate & update enrolment (KeyedProcessFunction, no window)
    val processedStream = dedupProcess
      .getSideOutput(config.uniqueConsumptionOutput)
      .keyBy(event =>
        s"${event.getOrElse(config.userId, "")}_${event.getOrElse(config.courseId, "")}_${event.getOrElse(config.batchId, "")}"
      )
      .process(new ProgramActivityAggregatesFunctionV2(config, httpUtil))
      .name(config.programactivityAggregateUpdaterFn)
      .uid(config.programactivityAggregateUpdaterFn)
      .setParallelism(config.activityAggregateUpdaterParallelism)

    // ── Sink : cert-issue events ──────────────────────────────────────────────
    processedStream
      .getSideOutput(config.certIssueOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaCertIssueTopic))
      .name(config.certIssueEventProducer)
      .uid(config.certIssueEventProducer)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage — entry point only runs inside Flink cluster
object ProgramActivityAggregateUpdaterStreamTaskV2 {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath
      .map(path => ConfigFactory.parseFile(new File(path)).resolve())
      .getOrElse(
        ConfigFactory
          .load("program-activity-aggregate-updater.conf")
          .withFallback(ConfigFactory.systemEnvironment())
      )
    val taskConfig     = new ProgramActivityAggregateUpdaterConfigV2(config)
    val kafkaConnector = new FlinkKafkaConnector(taskConfig)
    val httpUtil       = new HttpUtil
    new ProgramActivityAggregateUpdaterStreamTaskV2(taskConfig, kafkaConnector, httpUtil).process()
  }
}
// $COVERAGE-ON$

