package nycTaxi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NYCTaxi extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("NY TAXI").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val taxiDF = spark.read.load("D:\\spark4\\src\\main\\resources\\data\\yellow_taxi_jan_25_2018")

    val taxiZonesDF = spark.read.format("csv").option("header","true").option("inferSchema","true")
      .load("D:\\spark4\\src\\main\\resources\\data\\taxi_zones.csv")


    // 1. Which zone has most pickup/dropoff overall?

    val mostPickZones = taxiDF.groupBy(col("PULocationID")).agg(count("*").as("total_trips"))
      .orderBy(col("total_trips").desc_nulls_last)
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"), joinType = "inner")
      .drop("LocationID","service_zone")

    //    mostPickZones.show()

    // 2. What are the peak hours for taxi ?


    val peakHours = taxiDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
      .groupBy(col("hour_of_day")).agg(count("*").as("number_of_trips"))
      .orderBy(col("number_of_trips").desc_nulls_last)

    //    peakHours.show()


    // 3. How are the trips are distributed by length ?

    val distanceThreshold = 30

    val tripDistribution = taxiDF.withColumn("isLong",col("trip_distance") > distanceThreshold)
      .groupBy("isLong").agg(count("*"))

    //    tripDistribution.show()

    // 4. What are the peak hours for long/short trips ?

    val peakLongTripHours = taxiDF.select(col("*")).where(col("trip_distance") > distanceThreshold)
      .withColumn("hourss",hour(col("tpep_pickup_datetime")))
      .groupBy(col("hourss")).agg(count("*").as("total"))
      .orderBy(col("total").desc_nulls_last)

        //    peakLongTripHours.show()

    val peakShortTripHours = taxiDF.select(col("*")).where(col("trip_distance") < distanceThreshold)
      .withColumn("hourss",hour(col("tpep_pickup_datetime")))
      .groupBy(col("hourss")).agg(count("*").as("total"))
      .orderBy(col("total").desc_nulls_last)

    //    peakShortTripHours.show()

    //    peakLongTripHours.show()


    // 5. What are the top 3 pickup/drop off locations for long/short trips ?

    val picknDropLongTrips = taxiDF.select("*").where(col("trip_distance") > distanceThreshold)
      .groupBy(col("PULocationID"),col("DOLocationID"))
      .agg(count("*").as("total_trips"))
      .orderBy(col("total_trips").desc_nulls_last)
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone","pickup_location")
      .drop("LocationID","Borough","service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone","dropoff_location")
      .drop("LocationID","Borough","service_zone")

//    picknDropLongTrips.show()


    val picknDropShortTrips = taxiDF.select("*").where(col("trip_distance") < distanceThreshold)
      .groupBy(col("PULocationID"),col("DOLocationID"))
      .agg(count("*").as("total_trips"))
      .orderBy(col("total_trips").desc_nulls_last)
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone","pickup_location")
      .drop("LocationID","Borough","service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone","dropoff_location")
      .drop("LocationID","Borough","service_zone")

//    picknDropShortTrips.show()


    // 6. How are people paying for long and short trips ?

    val popularPaymentMethod = taxiDF.groupBy(col("RatecodeID"))
      .agg(count("*").as("payment_mode"))
      .orderBy(col("payment_mode").desc_nulls_last)

//    popularPaymentMethod.show()


    // 7. How is payment type evolving with time ?

    val paymentEvolutionDF = taxiDF.groupBy(to_date(col("tpep_pickup_datetime")), col("RatecodeID"))
      .agg(count("*").as("total_numbers"))
      .orderBy(to_date(col("tpep_pickup_datetime")))

    paymentEvolutionDF.show()
}
