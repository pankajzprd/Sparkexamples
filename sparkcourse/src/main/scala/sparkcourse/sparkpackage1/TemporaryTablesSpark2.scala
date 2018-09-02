package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
 * Starting Spark 2, there are 3 options available to create temporary tables in Spark
 * createTempView -- Creates a new view. errors if the view is already present
 * createOrReplaceTempView -- Either creates or replaces existing temporary view 
 * createGlobalTempView -- Creates a view whose lifetime depends on the spark application
 */
object TemporaryTablesSpark2 {
  def main(args: Array[String]): Unit = {
    
    // Creating Spark Session
    val sparksession = SparkSession.builder()
    .appName("Creating Temporary Table in Spark 2")
    .master("local")
    .getOrCreate()
    
    // Setting Log level
    Logger.getLogger("org").setLevel(Level.FATAL)
    
    val rainfall = sparksession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/test/resources/datasets/csv/rainfall in india 1901-2015.csv")
    
    rainfall.printSchema()
    
    rainfall.createTempView("rainfall")
    
    val select = sparksession.sql("select * from rainfall where subdivision ='VIDARBHA' limit 100")
    val select1 = sparksession.sql("select * from rainfall where year = 2012 limit 20")
    .createOrReplaceTempView("year2012")
    
    val x = sparksession.sql("select * from year2012").show()
    
    select.createOrReplaceTempView("rainfall")
    val newtable = sparksession.sql("select * from rainfall").toDF()
    newtable.show()

  }
}