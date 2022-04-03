package com.bgc.sparkInterPreter.analytics.impl

import com.bgc.sparkInterPreter.analytics.AnalyticsMetrics
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_set, rank}
import org.apache.spark.sql.{Dataset, Row}

/**
 *  For these 20 movies, list the persons who are most often credited and list the
    different titles of the 20 movies.
 */
class PersonMostCreditedWithMovieTitle extends AnalyticsMetrics{
  override def runAnalyticsMetrics(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row], principalsDataset: Option[Dataset[Row]],
                                   namesDataset: Option[Dataset[Row]]): Dataset[Row] = {
    val ratingDataFrameWithAverage = ratingsDataset.select("tconst","averageRating","numVotes").groupBy("tconst").
      agg(avg("numVotes").alias("averageNumberOfVotes"))

    val joinRating = ratingsDataset.join(ratingDataFrameWithAverage,"tconst")

    val ratingSpecDF = joinRating.withColumn("ratingSpec",col("numVotes").divide(col("averageNumberOfVotes"))
      .multiply(col("averageRating")))

    val windowSpec = Window.partitionBy(ratingSpecDF.col("tconst")).orderBy(ratingSpecDF.col("ratingSpec"))
    val filterRatingDF = ratingSpecDF.withColumn("rank",rank().over(windowSpec)).filter(col("numVotes") > 50).limit(20)

    val filterMovieDF = filterRatingDF.join(moviesDataset,"tconst")
    filterMovieDF.show()
    //Get Person most often credited along with Movie name
    val moviePrincipalDF = filterMovieDF.join(principalsDataset.get,"tconst")
      .select("tconst","nconst","originalTitle","primaryTitle")
    moviePrincipalDF.show()
    val moviePersonCreditedWithDIffNameDF = moviePrincipalDF.join(namesDataset.get,"nconst")
      .select("tconst","nconst","originalTitle","primaryName","primaryTitle")

    //List Of Persons most often credited along with Different titles
    val result = moviePersonCreditedWithDIffNameDF.groupBy("originalTitle").agg(collect_set(col("primaryName")).as("Persons most often credited"),
      collect_set(col("primaryTitle")).as("Different titles"))
    result.show()
    result
  }
}
