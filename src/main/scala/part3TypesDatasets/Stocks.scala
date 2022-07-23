package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Stocks extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregations and Grouping").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val stocksDf = spark.read.format("csv").option("header","true").load("D:\\spark4\\src\\main\\resources\\data\\stocks.csv")

  val stockWithDateFormat = stocksDf.select(col("symbol"),col("date"), to_date(col("date"), "MMM dd yyyy"))
  stockWithDateFormat.show()
  stockWithDateFormat.printSchema()

}
