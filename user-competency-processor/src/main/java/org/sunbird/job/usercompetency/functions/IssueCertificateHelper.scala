package org.sunbird.job.usercompetency.functions

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.usercompetency.task.UserCompetencyPreProcessorConfig
import org.sunbird.job.util.{HttpUtil, ScalaJsonUtil}

trait IssueCertificateHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserCompetencyPreProcessorFn])

  def getAPICall(url: String, responseParam: String)(config: UserCompetencyPreProcessorConfig, httpUtil: HttpUtil, metrics: Metrics): Map[String, AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if (200 == response.status) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse(responseParam, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if (400 == response.status && response.body.contains(config.userAccBlockedErrCode)) {
      metrics.incCounter(config.skippedEventCount)
      logger.error(s"Error while fetching user details for ${url}: " + response.status + " :: " + response.body)
      Map[String, AnyRef]()
    } else {
      throw new Exception(s"Error from get API : ${url}, with response: ${response}")
    }
  }
}
