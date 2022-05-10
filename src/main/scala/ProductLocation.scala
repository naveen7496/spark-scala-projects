import org.apache.spark.{SparkConf, SparkContext}

object ProductLocation extends App {
  val conf = new SparkConf().setAppName("Products and location").setMaster("local[*]")
  val sparkContext = new SparkContext(conf)

  val users = sparkContext.textFile("D:\\spark4\\data_folder\\user.txt")
    .map(x => x.split("\t")).map(x => (x(0),x(3)))


  val transactions = sparkContext.textFile("D:\\spark4\\data_folder\\transaction.txt")
    .map(x => x.split("\t")).map(x => (x(2),x(1)))


  val jn = transactions.leftOuterJoin(users).values.distinct
  val results = jn.countByKey
  jn.collect.foreach(println)
  println(results)
}
