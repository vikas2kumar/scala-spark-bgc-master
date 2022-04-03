package com.bgc.sparkInterPreter

import org.apache.spark.sql.SparkSession

/**
 * This trait is provide the spark session for application
 */
trait SparkSessionProvider {
  def sparkSession: SparkSession
  val userPath:String
}
