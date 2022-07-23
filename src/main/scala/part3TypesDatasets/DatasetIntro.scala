package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

//import java.util.Date

object DatasetIntro extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Dataset Intro")
      .config("spark.master","local")
      .getOrCreate()

    val numbersDf = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("D:\\spark4\\src\\main\\resources\\data\\numbers.csv")

    numbersDf.printSchema()
//    numbersDf.filter(col("numbers") > 619973).show()

    // DATASETS

    implicit val intEncoder = Encoders.scalaInt

    val numbersDS = numbersDf.as[Int]

    numbersDS.filter(_ < 619973)

    // CONVERT DF TO DS OR DATASET OF COMPLEX TYPE

    // CREATE CASE CLASS

    //"Name":"chevrolet chevelle malibu", "Miles_per_Gallon":18, "Cylinders":"Unknown", "Displacement":307, "Horsepower":130,
    // "Weight_in_lbs":3504, "Acceleration":12, "Year":"1970-01-01", "Origin":"USA"

    case class Car(
                    Name : String,
                    Miles_per_Gallon: Option[Double],   // Option is for Handling NULL in Datasets
                    Cylinders : Option[Double],
                    Displacement : Option[Double],
                    Horsepower : Option[Double],
                    Weight_in_lbs : Option[Double],
                    Acceleration : Option[Double],
                    Year : Date,
                    Origin : String
                  )

    import spark.implicits._

    def readDf(filename : String) = spark.read.format("json").option("inferSchema","true")
      .load(s"D:/spark4/src/main/resources/data/${filename}")

    val carsDf = readDf("cars.json")

//    carsDf.show()
//    carsDf.printSchema()

    val carsDS = carsDf.as[Car]
//    carsDS.printSchema()

//    println(carsDS
//      .select("Name").count())
//      .show()
    // COUNT OF CARS

//    carsDS.select(count(col("Horsepower"))).show()

    // POWERFUL CARS HP > 140
    carsDS.select("*").where(col("Horsepower") > 140).show()
//    println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)


    // AVERAGE HORSE POWER FOR ENTIRE DATASET

//    carsDS.select(avg(col("Horsepower")).as("Average HP")).show()




    //JOIN IN DATASETS

    val guitarPlayerDf = readDf("guitarPlayer.json")
    val guitarDf = readDf("guitars.json")
    val bandDf = readDf("bands.json")

    case class GuitarPlayer(
                             id : Long,
                             name : String,
                             guitars : Seq[Long],
                           band : Long
                           )

//    "id":0,"model":"EDS-1275","make":"Gibson","guitarType":"Electric double-necked"
    case class Guitar(
                       id : Long,
                       model : String,
                       make : String,
                       guitarType : String
                     )

//    {"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}

    case class Band(
                   id : Long,
                   name : String,
                   hometown : String,
                   year : Long
                   )

    val guitarPlayersDS = guitarPlayerDf.as[GuitarPlayer]
    val guitarDS = guitarDf.as[Guitar]
    val bandDS = bandDf.as[Band]

    guitarDS.show()
    guitarPlayersDS.show()
    bandDS.show()

    val joinedGuitarAndPlayers = guitarPlayersDS.joinWith(guitarDS,
        array_contains(guitarPlayersDS.col("guitars"),guitarDS.col("id")), "outer")

    joinedGuitarAndPlayers.show()





}
