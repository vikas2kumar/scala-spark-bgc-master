package com.bgc.sparkInterPreter

import com.bgc.sparkInterPreter.analytics.impl.{BestMoviesByVoting, PersonMostCreditedWithMovieTitle}
import org.apache.spark.sql.SparkSession

object AnalyticsApp extends App with SparkSessionProviderModule {

   val MOVIES_CSV_FILE = "/dataset/title.basics.tsv"
   val RATINGS_CSV_FILE = "/dataset/title.ratings.tsv"
   val TITLE_PRINCIPAL_CSV_FILE ="/dataset/title.principals.tsv"
   val NAME_BASICS_CSV_FILE="/dataset/name.basics.tsv"

   val moviesDataset =readCsv(sparkSession,userPath.concat(MOVIES_CSV_FILE))
   val ratingsDataset = readCsv(sparkSession, userPath.concat(RATINGS_CSV_FILE))
   val titlePrincipalDataset = readCsv(sparkSession, userPath.concat(TITLE_PRINCIPAL_CSV_FILE))
   val nameBasicsDataset = readCsv(sparkSession, userPath.concat(NAME_BASICS_CSV_FILE))

   /**
    * This method create DataFrame for given csv
    * @param s
    * @param fullPath
    * @return
    */
   def readCsv(s: SparkSession, fullPath: String) = {
      s.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", value = true)
        .option("sep", "\t")
        .load(fullPath)
   }

   /**
    *  Application run the analytics
    */
   new BestMoviesByVoting().runAnalyticsMetrics(moviesDataset,ratingsDataset).show()
   new PersonMostCreditedWithMovieTitle().runAnalyticsMetrics(moviesDataset,ratingsDataset,
      Some(titlePrincipalDataset),Some(nameBasicsDataset)).show()

}
