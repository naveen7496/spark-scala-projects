import org.apache.spark.{SparkConf, SparkContext}

object ShakespearWordCountAss1 extends  App {

  val conf = new SparkConf().setAppName("Shakespear Word Count").setMaster("local[*]")
  val sparkContext = new SparkContext(conf)

  val rdd1 = sparkContext.textFile("D:\\spark4\\Shakespeare.txt")
  val rdd2 = rdd1.flatMap(x => x.split("\\W"))
  val rdd3 = rdd2.map(word => (word,1))
  val rdd4 = rdd3.reduceByKey((x,y) => (x+y))  //word count rdd
  val rdd5 = rdd4.filter(_._2 > 100) // widely used words
  val rdd6 = rdd4.filter(_._2 < 30) // rarely used words
  val rdd7 = rdd4.sortBy(_._2,false)
  rdd7.collect().foreach(println)
}
