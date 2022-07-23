package part4SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import part1recap.SmartPhones
import part1recap.SmartPhones.moviesDf

object SparkSql extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("SPARK SQL")
      .config("spark.master","local")
      .getOrCreate()


    val carsDf = spark.read.format("json").option("inferSchema","true")
      .load("D:\\spark4\\src\\main\\resources\\data\\cars.json")

//    carsDf.printSchema()

    // USE SPARK SQL

    carsDf.createOrReplaceTempView("cars")
    val carSQLDf = spark.sql(
        """
          |SELECT * FROM cars WHERE Origin != 'USA'
          |""".stripMargin)

//    carSQLDf.show()

//    spark.sql("""CREATE TABLE mySparkTAble""")


//    moviesDf.write.format("jdbc")
//      .options(Map(
//          "driver" -> "org.postgresql.Driver",
//          "url"-> "jdbc:postgresql://localhost:5432/testing",
//          "user"-> "postgres",
//          "password"-> "1234",
//          "dbtable"-> "public.movies"
//      )).save()


    val carsSalesDf = spark.read.format("jdbc")
    .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/testing")
        .option("user","postgres")
        .option("password","1234")
        .option("dbtable","car_sales").load()

        carsSalesDf.createOrReplaceTempView("carsSales")

    val cars = spark.sql(
        """
          |SELECT car_name, SUM(sales)
          |FROM carsSales
          |GROUP BY car_name
          |ORDER BY car_name
          |""".stripMargin)

    cars.show()
}
