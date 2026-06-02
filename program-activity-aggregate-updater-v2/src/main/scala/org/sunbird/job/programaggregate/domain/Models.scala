package org.sunbird.job.programaggregate.domain

import java.util.Date

case class ContentStatus(contentId: String, status: Int = 0, completedCount: Int = 0, viewCount: Int = 1, fromInput: Boolean = true, eventsFor: List[String] = List())

case class UserContentConsumption(userId: String, batchId: String, courseId: String, contents: Map[String, ContentStatus])

case class UserActivityAgg(activity_type: String,
                           user_id: String,
                           activity_id: String,
                           context_id: String,
                           aggregates: Map[String, Double],
                           agg_last_updated: Map[String, Long])

case class CollectionProgress(userId: String, batchId: String, courseId: String, progress: Int, completedOn: Date, contentStatus: Map[String, Int], inputContents: List[String], completed: Boolean = false)

