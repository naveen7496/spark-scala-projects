//import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object FileReader extends App {

  case class Employee(id:Int,fname: String, email:String,age:Int,salary:Int)

  val conf = new SparkConf().setAppName("My Demo").setMaster("local[*]")
  conf.set("spark.driver.allowMultipleContexts","true")
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

//    val sc = new SparkContext(conf)

  //  val fileRDD = sc.textFile("D:\\scala\\MOCK_DATA.csv")
  //  val rddHeader = fileRDD.first()
  //  val fileReaderWithoutHeader = fileRDD.filter(row => row != rddHeader)

  //  fileRDD.collect().foreach(println)
  //    val schemaRDD = fileReaderWithoutHeader.map(row => row.split(","))
  //      .map(ar => Employee(ar(0).toInt, ar(1), ar(2), ar(3).toInt, ar(4).toInt))
  //  schemaRDD.collect().foreach(println)

  val df =  sparkSession.read.option("header","true")
      .option("inferSchema","true")
      .csv("D:\\scala\\MOCK_DATA.csv")

//  df.show()
//df.printSchema()
//  df.select("id","first_name").show(5)
//  df.show(7)
//  df.select("id","first_name", "age").show(35)

//  df.show(60)
//  df.filter("age =='70'").show()
//  df.groupBy("age").count().show()
//df.groupBy("age").count().filter("count > 5").show()
//df.groupBy("age").count().orderBy("count").show(100)
df.filter("age > 30 and age < 70").show(100)
}
