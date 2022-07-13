package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MoviesExerciseAggregation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregation Exercise on Movies").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDataframe = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

//  moviesDataframe.show()

  // SUM UP ALL THE PROFITS OF MOVIES

  val totalProfit = moviesDataframe.select((col("US_Gross") + col("Worldwide_Gross")).as("totalProfit"))
//  totalProfit.select(sum("totalProfit")).show()

  // COUNT OF DISTINCT DIRECTORS

//  moviesDataframe.select(countDistinct("Director")).show()
//  moviesDataframe.select("Director").distinct().show()


  // MEAN AND STANDARD DEVIATION OF US GROSS

//  moviesDataframe.select(mean("US_Gross"), stddev("US_Gross")).show()


  // AVERAGE IMDB RATING AND US GROSS PER DIRECTOR

  moviesDataframe.groupBy("Director").agg(avg("IMDB_Rating").as("avg_IMDB"),
    avg("US_Gross").as("avg_gross"))
    .orderBy("avg_gross")
    .show()


  moviesDataframe.groupBy("Director").agg(avg("IMDB_Rating").as("avg_IMDB"),
    avg("US_Gross").as("avg_gross"))
    .orderBy(col("avg_IMDB").desc_nulls_last)
    .show()

}
