package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CarsParse extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Cars").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    val carsDf = spark.read.json("D:\\spark4\\src\\main\\resources\\data\\cars.json")
    val carsDf = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\cars.json")
//    carsDf.show()

//    carsDf.printSchema()
//    carsDf.take(5).foreach(println)
//        println(carsDf.schema)


    val carSchema = StructType(Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders",IntegerType),
        StructField("Displacement",IntegerType),
        StructField("Horsepower",IntegerType),
        StructField("Weight_in_lbs",IntegerType),
        StructField("Acceleration",DoubleType),
        StructField("Year",StringType),
        StructField("Origin",StringType)

    ))

//    val carsSchemaDF = spark.read.format("json").schema(carSchema).option("mode","dropMalformed")
//      .load("D:\\spark4\\src\\main\\resources\\data\\cars.json")
//    carsSchemaDF.show()

    val carsSchemaDF = spark.read.format("json").schema(carSchema).options(Map(
        "mode" -> "dropMalformed",
        "path" -> "D:\\spark4\\src\\main\\resources\\data\\cars.json"
    )).load()

    import spark.implicits._

    // create df from tuples

    val carsTuples = Seq(
        ("Alto","Maruti","India"),
        ("City","Honda","Japan"),
        ("M8","BMW","Germany")
    )
    val manualCarsDf = spark.createDataFrame(carsTuples)
//    manualCarsDf.show()
//    manualCarsDf.printSchema()

    val manualCarsImplicitDf = carsTuples.toDF("Name","Company","COO")
//    manualCarsImplicitDf.show()
//    manualCarsImplicitDf.printSchema()

//    carsSchemaDF.show(50 )

//    carsSchemaDF.write.format("csv")
//      .mode(SaveMode.Overwrite)
//      .save("D:\\spark4\\src\\main\\resources\\data\\carsDuplicate")

//    carsSchemaDF.write.mode(SaveMode.Overwrite).parquet("D:\\spark4\\src\\main\\resources\\data\\carsDuplicateparquet")
        val parqDF = spark.read.parquet("D:\\spark4\\src\\main\\resources\\data\\carsDuplicateparquet\\parquet_test.parquet")
//    parqDF.show(5)

    val carSales = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/testing")
      .option("user","postgres")
      .option("password","1234")
      .option("dbtable","car_sales")
      .load()
    carSales.show()




















}

