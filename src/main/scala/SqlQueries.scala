import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlQueries extends App {
    val conf = new SparkConf().setAppName("Sql Queries").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val myDf = sparkSession.read.option("header","true").option("inferschema","true").csv("D:\\scala\\MOCK_DATA.csv")

    myDf.show()
//  myDf.createOrReplaceTempView("tbl_employee")
//  sparkSession.catalog.listTables().show()
//sparkSession.sql("show tables").show()
//  sparkSession.sql("desc tbl_employee").show()
//  sparkSession.sql("SELECT * FROM tbl_employee").show(89)
//  sparkSession.sql("SELECT * FROM tbl_employee WHERE age = 34").show()
//sparkSession.sql(
//  """SELECT count(*) as age_count
//    |FROM tbl_employee WHERE age = 34
//    |""".stripMargin).show()
//  sparkSession.sql("SELECT * FROM tbl_employee WHERE first_name LIKE '%ni%'").show()
//sparkSession.sql("SELECT * FROM tbl_employee WHERE id = 19").show()
//sparkSession.sql("SELECT age, count(*) AS C FROM tbl_employee GROUP BY age ORDER BY age").show(100)
//val jsonData = sparkSession.read.json("D:\\scala\\MOCK_DATA.json")
//
//  jsonData.show()
//jsonData.write.csv("D:\\scala\\jsontocsv.csv")

}
