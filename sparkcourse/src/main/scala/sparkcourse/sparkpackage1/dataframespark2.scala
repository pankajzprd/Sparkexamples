package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object dataframespark2 {
  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder()
      .appName("Creating DataFrame using Spark 2")
      .master("local")
      .getOrCreate()

    val valueRDD = sparksession.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8), 2)

    println("Printing values from RDD")
    valueRDD.foreach(println)
    val schema = StructType(
      StructField("Integer Values", IntegerType, true) :: Nil)

    // Creating Row RDD to create DataFrame
    val rowRDD = valueRDD.map(line => Row(line))

    // Creating DataFrame
    val DF = sparksession.createDataFrame(rowRDD, schema)
    println("Printing Schema of DataFrame")
    DF.printSchema()
    println("Printing values from DataFrame")
    DF.show()
  }
}