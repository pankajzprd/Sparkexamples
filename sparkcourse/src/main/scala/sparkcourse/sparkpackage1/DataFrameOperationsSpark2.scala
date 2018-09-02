package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object DataFrameOperationsSpark2 {

  def main(args: Array[String]): Unit = {
 Logger.getLogger("org").setLevel(Level.FATAL)
    val sparksession = SparkSession.builder()
      .appName("Working on DF Operations")
      .master("local")
      .getOrCreate()

    val Schema = StructType(
      StructField("Id", LongType, true) ::
        StructField("Name", StringType, true) ::
        StructField("Year", LongType, true) ::
        StructField("Gender", StringType, true) ::
        StructField("State", StringType, true) ::
        StructField("Count", LongType, true) :: Nil)

    val StateNames = sparksession
      .read
      .schema(Schema)
      .option("header", "true")
      .csv("src/test/resources/datasets/csv/StateNames.csv")

   StateNames.persist(StorageLevel.MEMORY_AND_DISK)
   
    val schemanew = StateNames.schema
    println(schemanew)

    val columns = StateNames.columns
    println(columns.mkString(" |"))
    
    val coldescr = StateNames.describe("State")
    coldescr.show()
    
    val datatypes = StateNames.dtypes
    println(datatypes.mkString(" , "))
    
    StateNames.head(10).foreach(println)

    val selectdata = StateNames.select("Id", "Name", "Gender", "State").toDF()
    selectdata.show(10)
    
    println("Where Clause ")
    
    val wheredata = StateNames.select("*").where("Gender == 'F'").show(20)
    
    println("Filter Clause")
    val filterdata =  StateNames.select("*").filter(StateNames("Gender") === "F").toDF()
    
    filterdata.show(10)
    
    println("Group data")
    
    val groupdata = selectdata.groupBy("State").sum("Id").show()
    println("Distinct values")
    val distinctdata = selectdata.distinct().show()
    
    println("repartition data")
    selectdata.repartition(2).show()
    sparksession.close()
  }
}
