package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

/**
 * Spark claims Datasets are 50% faster than DataFrames
 * We'll take starttime and end time to verify it by performing same operations on
 * both Datasets and DataFrames
 */
case class athelets(ID: Long, Name: String, Sex: String, Age: String,
                    Height: String, Weight: String, Team: String, NOC: String, Games: String,
                    Year: String, Season: String, City: String, Sport: String,
                    Event: String, Medal: String)
object DFvsDS {
  def main(args: Array[String]): Unit = {
    
    val sparksession = SparkSession.builder()
      .appName("Comparing DataFrame and DataSet")
      .master("local")
      .getOrCreate()

    val starttimeDF = System.currentTimeMillis()

    val atheletesDF = sparksession
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/datasets/csv/athlete_events.csv")

    atheletesDF.printSchema();
    val filterNOCDF = atheletesDF.filter("Sex == 'M'")
    println("The total count for NOC -- USA in DataFrame is : " + filterNOCDF.count())
   
    val endtimeDF = System.currentTimeMillis()  

    import sparksession.implicits._
    val starttimeDS = System.currentTimeMillis()

    val atheletesDS = atheletesDF.as[athelets]
    atheletesDS.printSchema()
    val filterNOCDS = atheletesDS.filter("Sex == 'M'")
    println("The total count for Sex -- M in Dataset is : " + filterNOCDS.count())
   
    val endtimeDS = System.currentTimeMillis()

    println("Time Calculations")
    println("The time taken to perform count on DataFrame is : " + (endtimeDF - starttimeDF) / 1000)
    println("The time taken to perform count on Dataset is : " + (endtimeDS - starttimeDS) / 1000)

  }
}