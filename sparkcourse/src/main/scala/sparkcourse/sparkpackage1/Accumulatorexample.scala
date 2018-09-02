package sparkcourse.sparkpackage1
import org.apache.spark.sql.SparkSession
object Accumulatorexample {

  /**
   * Analyzes resources/purchases.log file.
   * Uses accumulators to detect various types of faulty records.
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("mapExample")
      .master("local")
      .getOrCreate()
      
    //  val sc = spark.sparkContext

    val badPkts = spark.sparkContext.longAccumulator("Bad Packets")
    val zeroValueSales = spark.sparkContext.longAccumulator("Zero Value Sales")
    val missingFields = spark.sparkContext.longAccumulator("Missing Fields")
    val blankLines = spark.sparkContext.longAccumulator("Blank Lines")


    spark.sparkContext.textFile("src/test/resources/datasets/purchases.log.txt", 4)
      .foreach { line =>

        if (line.length() == 0) blankLines.add(1)
        else if (line.contains("Bad data packet")) badPkts.add(1)
        else {
          val fields = line.split("\t")

          if (fields.length != 4) missingFields.add(1)
          else if (fields(3).toFloat == 0) zeroValueSales.add(1)
        }
      }

    println("Purchase Log Analysis Counters:")
    println(s"\tBad Data Packets=${badPkts.value}")
    println(s"\tZero Value Sales=${zeroValueSales.value}")
    println(s"\tMissing Fields=${missingFields.value}")
    println(s"\tBlank Lines=${blankLines.value}")
  }
}