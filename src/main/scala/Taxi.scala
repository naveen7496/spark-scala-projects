import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

  total.show()
  toll.show()

  val a = total.first().getDouble(0)
  val b = toll.first().getAs[Double](0)

  val toll_fraction = (b / a) * 100
  println(a)
  println(b)
  println(toll_fraction)


}
