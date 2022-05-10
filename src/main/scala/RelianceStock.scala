import FileReader.conf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RelianceStock extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Reliance stock analysis").setMaster("local[*]")
    conf.set("spark.driver.allowMultipleContexts","true")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val stockDf = sparkSession.read.option("header","true").option("inferschema","true").csv("D:\\spark4\\NSE_RELIANCE_5_1.csv")

    val stockDf2 = stockDf.select("time","open","high","low","close")
//    stockDf2.select(avg("close")).show()
    val stockDf3 = stockDf2.select(col("*"), substring(col("time"), 1, 10).as("only_start_date"))
//    stockDf3.groupBy("only_start_date").count().show()
    val st4 = stockDf3.select("*").groupBy("only_start_date").agg(max("high"))
//        st4.show()
    val joinedDS = st4.join(stockDf3, stockDf3("high") === st4("max(high)")
        && stockDf3("only_start_date") === st4("only_start_date"), "inner")
//    joinedDS.select("time", "max(high)").show()

    val lossDf = stockDf2.withColumn("lossDiff",stockDf2("low") - stockDf2("open"))
//    lossDf.select(sum("lossDiff")).show()

    val profitDf = stockDf2.withColumn("profitDiff",stockDf2("high") - stockDf2("open"))
    profitDf.select(sum("profitDiff")).show()


}
