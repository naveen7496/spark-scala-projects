package sparkPractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ColumnsAndExpressions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("ColumnsExpressions").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  val carsDF = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\cars.json")

  // SELECT A COLUMN (PROJECTION)

//  val name = carsDF.col("Name")
  val carsNameDf = carsDF.select(carsDF.col("Name"))
//  carsNameDf.show(10)

  val multipleCols = carsDF.select("Name","Cylinders","Displacement","Acceleration")
//  multipleCols.show()



  // EXPRESSIONS

  val weight_in_kgs = carsDF.col("Weight_in_lbs") / 2.2

  val carsDfWithWeight = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weight_in_kgs.as("weight_in_kgs"),
    expr("Acceleration * 10").as("over_Acceleation"),
    col("Acceleration")
  )

  val carExprdf = carsDF.selectExpr(
    "Name" ,
    "Acceleration",
    "Acceleration * 10",
    "Acceleration / 10"
  )

//  carExprdf.show()

//  carsDfWithWeight.show()


  // ADD A NEW COLUMN

  val carsdfwithNewColumn = carsDF.withColumn("Acceleratio100", col("Acceleration") * 100)

  // RENAMING A COLUMN

  val casDfWithRenamedColumn = carsdfwithNewColumn.withColumnRenamed("Acceleratio100","Acc_X_100")

//  carsdfwithNewColumn.show()
//  casDfWithRenamedColumn.show()


  // DROP COLUMN(s)

//  carsDfWithWeight.show()

 val carExprdfdroppedColumns = carsDfWithWeight.drop("over_Acceleation")

//  carExprdfdroppedColumns.show()

//  carsDF.show()

  // FILTER

//  val europeanCarsDf = carsDF.filter(col("Origin") =!= "USA")'
  val noneuropeanCarsDf = carsDF.where(col("Origin") =!= "Europe")


  // MULTIPLE FILTER

  val americanPowerfulcarsDf = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 200)
//  americanPowerfulcarsDf.show()



  val morecarsDF = spark.read.format("json").option("inferSchema","true").load("D:\\spark4\\src\\main\\resources\\data\\morecars.json")

//  println(carsDF.count())

  // UNION OF DFs
  val unionCarsdf = carsDF.union(morecarsDF)

//  println(unionCarsdf.count())




  // DISTINCT

//  val allCountries =
    println(carsDF.select("origin").distinct().count())
//  allCountries.show()


















}


