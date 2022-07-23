package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ComplexType extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Complex Type").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  moviesDf.show()
  moviesDf.printSchema()
//  moviesDf.select("Title","US_Gross","Worldwide_Gross").show()


  // DATE

  moviesDf.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("actual_date"))
    .withColumn("Today_date", current_date())
    .withColumn("right_now", current_timestamp())
    .withColumn("movie-age", datediff(col("Today_date"),col("actual_date"))/365)
    .where(col("actual_date").isNull).show()

}
