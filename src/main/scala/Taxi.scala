import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Taxi extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("The Taxi").setMaster("local[*]")
  val ss = SparkSession.builder().config(conf).getOrCreate()

  val t_df = ss.read.option("header","true").option("inferschema","true").csv("D:\\spark4\\data_folder\\Hadoop Datasets\\yellow.csv")
//  t_df.show()
    t_df.createOrReplaceTempView("tbl_taxi")
//    ss.catalog.listTables().show()
//    ss.sql("DESC tbl_taxi").show()


  //1. Total number of trips
//  ss.sql("SELECT COUNT(VendorID) FROM tbl_taxi").show()

  // 2. Total revenue generated from all trips
  val total = ss.sql("SELECT SUM(total_amount) FROM tbl_taxi")

  // 3. Fraction of amount paid to toll
  val toll = ss.sql("SELECT SUM(tolls_amount) FROM tbl_taxi")
//
//  total.show()
//  toll.show()
//
  val a = total.first().getDouble(0)
//  val b = toll.first().getAs[Double](0)
//
//  val toll_fraction = (b / a) * 100
//  println(toll_fraction)


  // 4. Fraction of amount paid to driver tip

  val driver_tip = ss.sql("SELECT SUM(tip_amount) FROM tbl_taxi").first().getDouble(0)

  val tip_fraction = (driver_tip / a) * 100
//  println(driver_tip)
//  println(tip_fraction)

  // 5. average trip amount

  val avgTripAmount = ss.sql("SELECT AVG(total_amount) FROM tbl_taxi").first().getDouble(0)
//  println(avgTripAmount)


  // 6. average trip distance
  val avgDistance = ss.sql("SELECT AVG(trip_distance) FROM tbl_taxi").first().getDouble(0)
//  println(avgDistance)

  // 7. how many different payment types
  val paymentTypes = ss.sql("SELECT DISTINCT payment_type FROM tbl_taxi")
//  paymentTypes.show()
//  println(paymentTypes.count())

//  ss.sql("SELECT pickup_datetime,payment_type,tip_amount FROM tbl_taxi WHERE payment_type = 4").show()



// 8.  For each payment type, display the following details:
//    a. Average fare generated
//  b. Average tip
//    c. Average tax – tax is stored in the column, mta_tax

val paymentAnalysis = ss.sql("SELECT payment_type,AVG(fare_amount),AVG(tip_amount),AVG(mta_tax) FROM tbl_taxi GROUP BY payment_type")
//  paymentAnalysis.show()

//  ss.sql("SELECT pickup_datetime,SUBSTRING(pickup_datetime,12,13) AS hour FROM tbl_taxi").show()
//    ss.sql("select date_format(timestamp pickup_datetime, 'hh') AS hour FROM tbl_taxi").show()
  val filteredData = ss.sql("SELECT pickup_datetime,HOUR(pickup_datetime) AS hour,total_amount FROM tbl_taxi")

  filteredData.createOrReplaceTempView("filteredData_tbl")

//  ss.sql("SELECT * FROM filteredData_tbl").show()
  ss.sql("SELECT hour, MAX(total_amount) FROM filteredData_tbl GROUP BY hour").select(max("MAX(total_amount)")).show()


}
