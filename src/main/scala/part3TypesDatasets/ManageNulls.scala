package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManageNulls extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregations and Grouping").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  val movieNonNullRating = moviesDf.select(col("Title"),col("Rotten_Tomatoes_Rating"),col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("eitherRatings")
  )

//  movieNonNullRating.show()

  // CHECK FOR NULLS

//  movieNonNullRating.select("Title", "Rotten_Tomatoes_Rating").where(col("Rotten_Tomatoes_Rating").isNull).show()

  // NULLS WHEN ORDERING

//  moviesDf.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last).show()


  // REMOVING NULLS

  moviesDf.select(col("Title"),col("Rotten_Tomatoes_Rating")).na.drop().show()
}


