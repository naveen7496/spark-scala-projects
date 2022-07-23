package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, exp, expr, size, split, struct, to_date}

object MoviesForStruct extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregations and Grouping").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  val moviesStructDf = moviesDf.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("profit"))
//  moviesStructDf.show()

//    moviesStructDf.select(col("Title"),col("profit"),col("profit").getField("US_Gross").as("US_profit")).show()

  //ARRAY

  val titleWords = moviesDf.select(col("Title"), split(col("Title"), " |,").as("Title_words"))
//  titleWords.show()

  titleWords.select(col("Title"),
    expr("Title_words[0]"),
    size(col("Title_words")),
    array_contains(col("Title_words"), "Love")
  ).show()

}
