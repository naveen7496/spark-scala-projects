package part1recap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SmartPhones extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("SmartPhoneParser").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val smartPhones = Seq(
    ("iphone 13", "Apple", "7X4", 20),
      ("Galaxy 13", "Samsung", "8X4", 25),
    ("Nord", "OnePlus", "7X5", 26)
  )

  import spark.implicits._

  val smartPhoneDF = smartPhones.toDF("Make","Model","Dimension","Mega Pixels")
  smartPhoneDF.printSchema()
  smartPhoneDF.show()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")
//  moviesDf.printSchema()
//  val dfLength = moviesDf.count()
//  println(s"The movies d has ${dfLength} rows.")

//  "driver","org.postgresql.Driver")
//  .option("url","jdbc:postgresql://localhost:5432/testing")
//    .option("user","postgres")
//    .option("password","1234")
//    .option("dbtable","car_sales")




  moviesDf.write.format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url"-> "jdbc:postgresql://localhost:5432/testing",
      "user"-> "postgres",
      "password"-> "1234",
      "dbtable"-> "public.movies"
    )).save()


}
