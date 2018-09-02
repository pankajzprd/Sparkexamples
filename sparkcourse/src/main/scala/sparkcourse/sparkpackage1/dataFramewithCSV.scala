package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession;

object dataFramewithCSV {
  def main(args: Array[String]): Unit = {
    
    val sparksession = SparkSession.builder()
    .appName("Creating DataFrame with CSV Files")
    .master("local")
    .getOrCreate()
    
    /**
     * Option is used to avoid the header row and take the first line as schema or column names 
     **/
    val zomato = sparksession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/test/resources/datasets/csv/zomato.csv")
    
    /**
     * Another way to avoid header and provide schema is through Map function use
     */
    val calls_911 = sparksession
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("src/test/resources/datasets/csv/911.csv")
    
    // Printing Zomato details
    zomato.printSchema()
    zomato.show(10)
    
    // Printing 911 calls details
    calls_911.printSchema()
    calls_911.show(10)
    
    /**
     * Another way to avoid header and infer schema is by using properties file
     */
    
    val properties = Map("header" -> "true", "inferSchema" -> "true")
   
    val india_district_census = sparksession
    .read
    .options(properties)
    .csv("src/test/resources/datasets/csv/india-districts-census-2011.csv")
    
    india_district_census.printSchema()
    india_district_census.show(10)
  }
}