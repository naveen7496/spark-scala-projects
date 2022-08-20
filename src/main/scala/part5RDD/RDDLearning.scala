package part5RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RDDLearning extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("RDD Intro")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext

  val numberRDD = sc.parallelize(1 to 10)
//  numberRDD.foreach(println)

  val stockRDD = sc.textFile("D:\\spark4\\src\\main\\resources\\data\\stocks.csv")

  import spark.implicits._
//  stockRDD.map(line => line.split(",")).collect().foreach(println)

  val repartitionedStocksRDD = stockRDD.repartition(15)
//  repartitionedStocksRDD.toDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("D:\\spark4\\src\\main\\resources\\data\\stocks")
//  stockRDD.foreach(println)

//  val coalesceRDD = repartitionedStocksRDD.coalesce(7)
//  coalesceRDD.toDF.write.mode(SaveMode.Overwrite)
//    .parquet("D:\\spark4\\src\\main\\resources\\data\\stocksCoalesce")


  // 1. CREATE MOVIE RDD WITH FIELDS title,genre,rating

  val movieDf = spark.read.format("json").option("inferSchema","true")
    .load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  case class Movie(title : String, genre : String, rating : Double)

  val filteredMovieDf = movieDf.select(col("Title").as("title"),
                                      col("Major_Genre").as("genre"),
                                      col("IMDB_Rating").as("rating"))
    .where(col("Major_Genre").isNotNull and
      col("IMDB_Rating").isNotNull)
  val movieRDD = filteredMovieDf.as[Movie].rdd

//  movieRDD.foreach(println)

  // 2. SHOW DISTINCT GENRE

//  println(movieRDD.map(_.genre).distinct().toDF.count())



  // 3. SELECT DRAMA MOVIES WITH RATING > 6

  val goodDramaRdd = movieRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  goodDramaRdd.toDF.show()
}
