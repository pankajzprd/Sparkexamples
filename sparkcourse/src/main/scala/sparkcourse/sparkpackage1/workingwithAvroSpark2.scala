package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object workingwithAvroSpark2 {
  /**
   * Spark by default does not support Avro File Format
   * We have to add external Jar or dependency in POM.xml
   * This dependency is provided by DataBricks
   */
   def main(args: Array[String]): Unit = {
     // Creating Spark 2 Session
     val sparksession = SparkSession.builder()
     .appName("Working with Avro file Spark 2")
     .master("local")
     .getOrCreate()
     
     /**
      * Spark does not have a direct API to read Avro File.
      */
     val readavro = sparksession
     .read
     .format("com.databricks.spark.avro")
     .load("src/test/resources/datasets/avro/twitter.avro")
     
     readavro.printSchema()
     readavro.show(20)
     
     // Saving the Avro file 
     readavro.write
     .format("com.databricks.spark.avro")
     .save("file:\\E:\\Downloads\\Datasets\\avro")
   }
}
