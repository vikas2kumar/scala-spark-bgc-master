package com.bgc.sparkInterPreter

import org.apache.spark.sql.SparkSession

/**
 * This is used to provide local spark session for test cases
 */
trait LocalSparkSessionProvider extends SparkSessionProvider {
  override def sparkSession: SparkSession = sparkSessionBuilder.getOrCreate()

  lazy val sparkSessionBuilder: SparkSession.Builder = SparkSession.builder()
    .appName("AnalyticsApp")
    .config("spark.master","local[2]")
    .config("spark.scheduler.mode","FAIR")
    .config("spark.yarn.queue","test")
    .config("spark.yarn.ui","true")
}
