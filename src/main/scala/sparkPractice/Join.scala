package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Join extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Joins").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val bandDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\bands.json")

  val guitarsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\guitars.json")
  val guitaristsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\guitarPlayer.json")

  val guitaristsBandDf = guitaristsDf.join(bandDf, guitaristsDf.col("band") === bandDf.col("id"), "inner")
  guitaristsBandDf.show()


  // LEFT JOIN

  val guitarBandLeftJoinDf = guitaristsDf.join(bandDf, guitaristsDf.col("band") === bandDf.col("id"), "left")
  guitarBandLeftJoinDf.show()


  // RIGHT JOIN

  val guitarBandRightJoinDf = guitaristsDf.join(bandDf, guitaristsDf.col("band") === bandDf.col("id"), "right")
  guitarBandRightJoinDf.show()


  // OUTER JOIN

  val guitarBandOuterJoinDf = guitaristsDf.join(bandDf, guitaristsDf.col("band") === bandDf.col("id"), "outer")
  guitarBandOuterJoinDf.show()



}
