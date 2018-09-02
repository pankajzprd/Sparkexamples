package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object creatingRDDs_1 {
  def main(args: Array[String]): Unit = {

    //Initializing the Spark Session
    val spark = SparkSession
      .builder()
      .appName("Different ways to create RDD")
      .master("local[*]")
      .getOrCreate()

    println("Creating RDD based on collection defined ")

    val collectionRDD = spark.sparkContext.parallelize(Seq(
      ("maths", 52),
      ("english", 75),
      ("science", 82),
      ("computer", 65),
      ("maths", 85)))

    collectionRDD.foreach(println)

    val ArrayRDD = spark.sparkContext.parallelize(Array("jan", "feb", "mar", "april", "may", "jun"), 3)

    ArrayRDD.foreach(println)

    println("Creating RDD based on external datasets")

    val externaldataRDD = spark
    .sparkContext
    .textFile("src/test/resources/datasets/csv/zomato.csv")
        
    val externaldata = spark
      .read
      .csv("src/test/resources/datasets/csv/zomato.csv")

    println("Total number of elements are " + externaldataRDD.count())
    println("Total number of elements are " + externaldata.count())
    
    externaldataRDD.take(10).foreach(println)

    println("Creating RDD based on another RDD")

    val header = externaldataRDD.first()
    val anotherRDD = externaldataRDD.filter(line => line != header)

    anotherRDD.take(10).foreach(println)

  }
}