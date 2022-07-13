package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, expr, max, mean, min, stddev, sum}

object Aggregations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregations and Grouping").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  val genreCount = moviesDf.select(count(col("Major_Genre")))

//  genreCount.show()

//  moviesDf.selectExpr("count(Major_Genre)").show()
//  moviesDf.select(count(("*"))).show()
//  moviesDf.select(countDistinct(col("Major_Genre"))).show()

//  moviesDf.select(approx_count_distinct(col("Major_Genre"))).show()  // does  not count row by row

//  moviesDf.printSchema()

  // MIN AND MAX

  val minIMDB_rating = moviesDf.select(min(col("IMDB_Rating")))
//  minIMDB_rating.show()

  val maxRottenTomatoes_rating = moviesDf.select(max(col("Rotten_Tomatoes_Rating")))
//  maxRottenTomatoes_rating.show()

  val totalCollection_us = moviesDf.select(sum(col("US_Gross")))
//  totalCollection_us.show()

  val avg_rating = moviesDf.select(avg(col("Rotten_Tomatoes_Rating")))
//  avg_rating.show()


  // MEAN AND STANDARD DEVIATION

//  moviesDf.select(mean(col("Rotten_Tomatoes_Rating")), stddev(col("Rotten_Tomatoes_Rating"))).show()


  // GROUP

  val countbyGenre = moviesDf.groupBy(col("Major_Genre")).count()

  countbyGenre.show()

  val avgIMDB_rating_genre = moviesDf.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  avgIMDB_rating_genre.show()

  // GROUP BY USING AGG

  val agg = moviesDf.groupBy(col("Major_Genre"))
    .agg(count("*").as("n_columns"),
      avg("IMDB_Rating")
    )

  agg.show()

}
