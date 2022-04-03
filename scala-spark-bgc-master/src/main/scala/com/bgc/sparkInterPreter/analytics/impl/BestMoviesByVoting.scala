package com.bgc.sparkInterPreter.analytics.impl

import com.bgc.sparkInterPreter.analytics.AnalyticsMetrics
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, rank}
import org.apache.spark.sql.{Dataset, Row}

/**
 * Retrieve the top 20 movies with a minimum of 50 votes with the ranking determined by:
(numVotes/averageNumberOfVotes) * averageRating
 */

class BestMoviesByVoting extends AnalyticsMetrics {
  override def runAnalyticsMetrics(moviesDataset: Dataset[Row], ratingsDataset: Dataset[Row], principalsDataset: Option[Dataset[Row]],
                                   namesDataset: Option[Dataset[Row]]): Dataset[Row] = {
    val ratingDataFrameWithAverage = ratingsDataset.select("tconst","averageRating","numVotes")
      .groupBy("tconst").
      agg(avg("numVotes").alias("averageNumberOfVotes"))
    val joinRating = ratingsDataset.join(ratingDataFrameWithAverage,"tconst")
    val ratingSpecDF = joinRating.withColumn("ratingSpec",col("numVotes").divide(col("averageNumberOfVotes"))
      .multiply(col("averageRating")))
    ratingSpecDF.printSchema()
    val windowSpec = Window.partitionBy(ratingSpecDF.col("tconst")).orderBy(ratingSpecDF.col("ratingSpec"))
    val filterRatingDF = ratingSpecDF.withColumn("rank",rank().over(windowSpec)).filter(col("numVotes") >= 50).limit(20)
    filterRatingDF.show()
    filterRatingDF.join(moviesDataset,"tconst").select("originalTitle")
  }

}
