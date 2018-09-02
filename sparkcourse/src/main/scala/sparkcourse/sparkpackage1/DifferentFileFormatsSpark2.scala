package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object DifferentFileFormatsSpark2 {
  def main(args: Array[String]): Unit = {
    /**
     * Spark 2 by default supports several file formats including
     * CSV, ORC, Parquet, Json, Text
     */
    
    // Creating Spark Session
    val sparksession = SparkSession.builder()
    .appName("Working with File Formats")
    .master("local")
    .getOrCreate()
    
    println("Way to read Json files in Spark 2")
    
    val readJson1 = sparksession
    .read
    .json("src/test/resources/datasets/json/file1.json")
    
    println("Reading Json Data")
    readJson1.printSchema()
    readJson1.show(10)
    
    val readJson2 = sparksession.read.json("src/test/resources/datasets/json/file2.json")
    println("Reading Json Data 2")
    readJson2.printSchema()
    readJson2.show(10)
    
    val readorc = sparksession.read.orc("src/test/resources/datasets/orc-file-11-format.orc")
    println("Reading ORC Data")
    readorc.printSchema()
    readorc.show(10)
    
    val readparquet = sparksession.read.parquet("src/test/resources/datasets/userdata1.parquet")
    println("Reading Parquet Data")
    readparquet.printSchema()
    readparquet.show()
    
    /**
     * Spark by default does not support Avro and XML file format
     * We need to add Spark_Avro and Spark_XML dependencies to pom.xml file provided by Databricks
     */
    
    val readxml = sparksession
    .read
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .load("src/test/resources/datasets/xml/books.xml")
    
    println("Reading XML File")
    readxml.printSchema()
    readxml.show(10)
    
    // Creating Temporary table to read data from XML file
    val xmldata = readxml.createTempView("books")
    val sqlxml = sparksession.sql("select * from books limit 30").show()
    
  }
}