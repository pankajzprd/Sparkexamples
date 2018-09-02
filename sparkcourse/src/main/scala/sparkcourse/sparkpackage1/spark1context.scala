package sparkcourse.sparkpackage1

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object spark1context {
  def main(args: Array[String]) {
    //  val sparkconf = new SparkConf()
    //  sparkconf.setAppName("Setting Spark 1 Context")
    //  sparkconf.setMaster("local*")
    //  val sparkcontext  = new SparkContext(sparkconf)

    val sparksession = SparkSession.builder()
      .appName("Spark Context 2")
      .master("local")
      .getOrCreate()

    val array1 = Array(1, 2, 3, 4, 5, 6, 7)

    val arrayRDD = sparksession.sparkContext.parallelize(array1, 2)
    arrayRDD.foreach(println)

  }
}