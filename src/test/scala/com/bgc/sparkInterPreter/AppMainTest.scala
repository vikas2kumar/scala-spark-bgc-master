package com.bgc.sparkInterPreter

import com.bgc.sparkInterPreter.AnalyticsApp.{readCsv, sparkSession}
import com.bgc.sparkInterPreter.analytics.impl.{BestMoviesByVoting, PersonMostCreditedWithMovieTitle}
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AppMainTest extends FunSpec with Matchers with LocalSparkSessionProvider with TestUtils {
  System.setProperty("hadoop.home.dir", this.getClass.getResource("/hadoop/winutils").getPath)
  val userPath: String = this.getClass.getResource("/dataset/").getPath
  val MOVIES_CSV_FILE = "/title.basics.tsv/data.tsv"
  val RATINGS_CSV_FILE = "/title.ratings.tsv/data.tsv"
  val TITLE_PRINCIPAL_CSV_FILE ="/title.principals.tsv/data.tsv"
  val NAME_BASICS_CSV_FILE ="/name.basics.tsv/data.tsv"

  val moviesDataset =readCsv(sparkSession,userPath.concat(MOVIES_CSV_FILE))
  val ratingsDataset = readCsv(sparkSession, userPath.concat(RATINGS_CSV_FILE))
  val titlePrincipalDataset = readCsv(sparkSession, userPath.concat(TITLE_PRINCIPAL_CSV_FILE))
  val nameBasicsDataset = readCsv(sparkSession, userPath.concat(NAME_BASICS_CSV_FILE))



 it("should be able to retrieve the top 20 movies with a minimum of 50 votes with the ranking"){
   val result=new BestMoviesByVoting().runAnalyticsMetrics(moviesDataset,ratingsDataset)
   assert(result.count()===20)

   result.collect() should contain theSameElementsAs Array(
     Row("Carmencita"),
     Row("Le clown et ses chiens"),
     Row("Pauvre Pierrot"),
     Row("Un bon bock"),
     Row("Blacksmith Scene"),
     Row("Chinese Opium Den"),
     Row("Corbett and Courtney Before the Kinetograph"),
     Row("Edison Kinetoscopic Record of a Sneeze"),
     Row("Miss Jerry"),
     Row("La sortie de l'usine Lumière à Lyon"),
     Row("Akrobatisches Potpourri"),
     Row("L'arrivée d'un train à La Ciotat"),
     Row("Le débarquement du congrès de photographie à Lyon"),
     Row("L'arroseur arrosé"),
     Row("Autour d'une cabine"),
     Row("Barque sortant du port"),
     Row("Italienischer Bauerntanz"),
     Row("Das boxende Känguruh"),
     Row("The Derby 1895"),
     Row("Les forgerons")
   )
 }
  it("should be able to list the persons who are most often credited"){
    val result=new PersonMostCreditedWithMovieTitle().runAnalyticsMetrics(moviesDataset,ratingsDataset,
      Some(titlePrincipalDataset),Some(nameBasicsDataset))
    result.collect() should contain theSameElementsAs Array(
      Row("Edison Kinetoscopic Record of a Sneeze",arrayField("William Heise", "William K.L. Dickson"),arrayField("Edison Kinetoscopic Record of a Sneeze")),
      Row("Chinese Opium Den",arrayField("William K.L. Dickson"),arrayField("Chinese Opium Den")),
      Row("Blacksmith Scene",arrayField("William K.L. Dickson"),arrayField("Blacksmith Scene")),
      Row("Corbett and Courtney Before the Kinetograph",arrayField("William Heise", "William K.L. Dickson"),arrayField("Corbett and Courtney Before the Kinetograph")),
      Row("Carmencita",arrayField("William Heise","Carmencita","William K.L. Dickson"),arrayField("Carmencita"))

    )

  }
}
