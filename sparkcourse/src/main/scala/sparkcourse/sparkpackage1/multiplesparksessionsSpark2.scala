package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession
/**
 * There is always one SparkContext per JVM. Multiple spark sessions are not advised in PROD
 * environment as they could lead to errors. 
 * For more details, see below JIRA 
 * https://issues.apache.org/jira/browse/SPARK-2243
 */
object multiplesparksessionsSpark2 {
  def main(args: Array[String]): Unit = {
    val sparksession1 = SparkSession.builder()
    .appName("Creating multiple Spark sessions in Spark 2")
    .master("local")
    .getOrCreate()
    
    val sparksession2 = SparkSession.builder()
    .appName("Creating multiple Spark sessions in Spark 2")
    .master("local")
    .getOrCreate()
    
    val csvRDD1 = sparksession1
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("src/test/resources/datasets/csv/athlete_events.csv")
    
    val csvRDD2 = sparksession2
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("src/test/resources/datasets/csv/noc_regions.csv")
    
    csvRDD1.show(10)
    csvRDD2.show(10)
  }
}