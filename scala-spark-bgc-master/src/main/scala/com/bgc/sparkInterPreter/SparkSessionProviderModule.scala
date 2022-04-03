package com.bgc.sparkInterPreter

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.control.Exception.allCatch
import scala.util.{Failure, Success, Try}

/**
 * This create create the spark session for application
 */
trait SparkSessionProviderModule extends SparkSessionProvider with LazyLogging {
 System.setProperty("hadoop.home.dir", this.getClass.getResource("/hadoop/winutils").getPath)
 val userPath = System.getProperty("user.dir")

 def sparkSession: SparkSession= getOrCreateSparkSession()


 private[this] def getOrCreateSparkSession(): SparkSession={
  createSession match{
   case Success(s)=> {
    logger.info("Created Spark Session Successfully")
    s
   }
   case Failure(e)=> logger.error("Unable to create Spark session",e)
   throw new RuntimeException(e)
  }
 }

 private[this] def createSession: Try[SparkSession]=allCatch withTry {
  val session = sparkSessionBuild.getOrCreate()
  if(session.sparkContext.isStopped)
   throw new RuntimeException("Spark context in newly created spark session was expected to be running")
   else session
  }


 val sparkSessionBuild: SparkSession.Builder = SparkSession.builder()
   .appName(ConfigProvider.applicationName)
   .config("spark.cores.max",3)
   .config("spark.scheduler.mode","FAIR")
   .config("spark.driver.cores",4)
   .master("local[1]")

}
