package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import sparkPractice.Join
import sparkPractice.Join.spark

object SparkPractice extends App {

      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)

      val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
      val spark = SparkSession.builder().config(conf).getOrCreate()

      val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

//      moviesDf.show()
      moviesDf.printSchema()

//      moviesDf.select(col("Title"),col("Director")).show()

//      moviesDf.select("Title","Major_Genre").where(col("Major_Genre") === "Horror").show()

//        moviesDf.select((col("US_Gross") + col("Worldwide_Gross")).as("total_income")).show()

//        moviesDf.withColumn("total_income", col("US_Gross") + col("Worldwide_Gross")).show()


//    moviesDf.select(countDistinct(col("Major_Genre"))).show()
//  moviesDf.select("Major_Genre").distinct().show()

//  moviesDf.select(max(col("IMDB_Votes"))).show()


  val totalIncome = moviesDf.withColumn("total_income", col("US_Gross") + col("Worldwide_Gross"))
//  totalIncome.groupBy("Director").sum("total_income").show(50)


  val bandDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\bands.json")

  val guitarsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\guitars.json")
  val guitaristsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\guitarPlayer.json")


  val guitaristDetailsDf = guitaristsDf.join(bandDf, guitaristsDf.col("band") === bandDf.col("id"), "inner").show()


}
