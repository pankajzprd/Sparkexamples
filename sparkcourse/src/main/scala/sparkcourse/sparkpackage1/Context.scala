package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkSession = SparkSession
    .builder()
    .appName("Session creation for Spark 2")
    .master("local[*]")
    .getOrCreate()
}