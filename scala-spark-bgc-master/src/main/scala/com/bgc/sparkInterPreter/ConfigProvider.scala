package com.bgc.sparkInterPreter

import com.typesafe.scalalogging.LazyLogging

object ConfigProvider extends LazyLogging {
  lazy val applicationName: String = Option(System.getProperty("spark.com.bgc.analytics.application.name")).getOrElse("AnalyticsName")
}

