package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object ImposingOwnSchemaSpark2 {
  def main(args: Array[String]): Unit = {
    // Creating Spark Session
    val sparksession = SparkSession.builder()
    .appName("Imposing Own Schema on Files")
    .master("local")
    .getOrCreate()
    
    /**
     * with Options keyword, we can specify the header 
     * and request Spark to infer the schema as in file
     */
    val NASA_Facilities  = sparksession
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("src/test/resources/datasets/csv/NASA_Facilities.csv")
    
    println("Schema by Spark")
    NASA_Facilities.printSchema()
    NASA_Facilities.show(10)
    /**
     * We can create our own schema and apply it to the file with StructType and StructField
     */
    
    val schema1 = StructType(
        StructField("Center",StringType,true) ::
        StructField("CenterSearchStatus",StringType,true) ::
        StructField("Facility",StringType,true) ::
        StructField("FacilityURL",StringType,true) ::
        StructField("IsOccupied",StringType,true) ::
        StructField("Status",StringType,true) ::
        StructField("URLLink",StringType,true) ::
        StructField("RecordDate",StringType,true) ::
        StructField("LastUpdated",StringType,true) ::
        StructField("Country",StringType,true) ::
        StructField("Location",StringType,true) ::
        StructField("City",StringType,true) ::
        StructField("State",StringType,true) ::
        StructField("Zip",StringType,true) :: Nil)
        
   val NASA_Facilities1 = sparksession
   .read
   .option("header", "true")
   .schema(schema1)
   .csv("src/test/resources/datasets/csv/NASA_Facilities.csv")
   
   println("Imposing Own Schema")
   NASA_Facilities1.printSchema()
   NASA_Facilities1.show(10)
   
   val StateNames = sparksession
   .read
   .option("header", "true")
   .option("inferSchema", "true")
   .csv("src/test/resources/datasets/csv/StateNames.csv")
   
   println("Schema By Spark")
   StateNames.printSchema()
   StateNames.show(10)
   StateNames.take(10).foreach(println)
   
   /**
    * Creating Own Schema
    */
   val Schema = StructType(
       StructField("Id",LongType,true) ::
       StructField("Name",StringType,true) ::
       StructField("Year",LongType,true) ::
       StructField("Gender",StringType,true) ::
       StructField("State",StringType,true) ::
       StructField("Count",LongType,true) :: Nil)
       
  val StateNames1 = sparksession
  .read
  .schema(Schema)
  .option("header", "true")
  .csv("src/test/resources/datasets/csv/StateNames.csv")
  
  println("Schema by StructType")
  StateNames1.printSchema()
  StateNames1.show(10)
  }
}