package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions.col

object MoviesExercises extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Movies df").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  moviesDf.show()
  moviesDf.printSchema()

  // SELECT COLUMNS

//  moviesDf.select(moviesDf.col("Title"),moviesDf.col("Production_Budget")).show()
//  moviesDf.select(col("Title"),col("IMDB_Votes"),col("Release_Date")).show()
//  moviesDf.select("Title","Worldwide_Gross","Rotten_Tomatoes_Rating","MPAA_Rating").show()


  // SUMMING UP COLUMNS

  val totalIncome = moviesDf.withColumn("totalincome", col("US_Gross") + col("Worldwide_Gross"))
  totalIncome.show()


  // FILTERING

  val topComedyMovies = moviesDf.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6 and col("Rotten_Tomatoes_Rating") > 70)
  topComedyMovies.select("Title", "Major_Genre","IMDB_Rating","Rotten_Tomatoes_Rating").show()

  // OR

  val toppComedyMovies = moviesDf.select("Major_Genre","IMDB_Rating").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

    toppComedyMovies.show()




}
