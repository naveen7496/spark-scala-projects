package part3TypesDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Aggregations and Grouping").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val moviesDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\movies.json")

  // ADD PLANE VALUES TO DF

  val df = moviesDf.select(col("Title"), lit(47).as("somevalue"))
//  df.show()

  // BOOLEANS

  val dramaFilter = col("Major_Genre") === "Drama"
  val highRatedFilter = col("IMDB_Rating") > 7
  val highRatedDramaFilter = dramaFilter and highRatedFilter

//  moviesDf.select("Title","Director","Major_Genre","IMDB_Rating").where(highRatedDramaFilter).show()

  val averageBothRating = moviesDf.select(col("Title"),

      ((col("IMDB_Rating") + col("Rotten_Tomatoes_Rating")/10 )/2).as("avgOfBoth"))

//    averageBothRating.

//      select("Title","avgOfBoth").filter(col("avgOfBoth") === 4.8).show()


  val carsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\cars.json")


  // STRING

  // Capitalize first letter
//  carsDf.select(initcap(col("Name"))).show()

  // contains
//  carsDf.select("*").where(col("Name").contains("chevrolet")).show()

  // contains with regex

  val regerString = "volkswagen|vw"

  carsDf.select(col("Name"),
    regexp_extract(col("Name"), regerString, 0).as("extractedString")
  ).where(col("extractedString") =!= "")



  // REPLACE

  carsDf.select(col("Name"),
    regexp_replace(col("Name"), regerString, "People's Car").as("replaced_name")
  ).where(col("replaced_name").contains("People's Car"))



  // EXERCISE

  def getCarsNames : List[String] = List("Volkswagen","Ford")

  val carsFilter = getCarsNames.map(_.toLowerCase()).mkString("|")

//  println(carsFilter)

  carsDf.select(col("Name"),
    regexp_extract(col("Name"), carsFilter,0).as("filteredCars"))
    .filter(col("filteredCars") =!= "").drop("filteredCars").show()






}
