package com.bgc.sparkInterPreter.analytics

import org.apache.spark.sql.{Dataset, Row}

/**
 * This is used to provide Analytics for given datasets
 */
trait AnalyticsMetrics {
  def runAnalyticsMetrics(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row]
                          ,principalsDataset:Option[Dataset[Row]]=None,
                          namesDataset:Option[Dataset[Row]]=None
                         ): Dataset[Row]
}
