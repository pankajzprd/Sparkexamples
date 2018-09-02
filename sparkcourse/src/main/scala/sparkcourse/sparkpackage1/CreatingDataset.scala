package sparkcourse.sparkpackage1
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * In Order to create a Dataset, we must specify the schema of the file we are using.
 * For this, we should use CASE CLASS
 */
case class suicides(state: String, Year: Long, Type_Code: String,
                    Type: String, Gender: String, Age_group: String, total: Long)

case class yeartype(Year: String, Type: String)

object CreatingDataset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparksession = SparkSession.builder()
      .appName("Creating Datasets")
      .master("local")
      .getOrCreate()

    /**
     * sparksession.implicits helps to import internal functionality to convert DF to DS
     */
    import sparksession.implicits._

    val suicidesIndia = sparksession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/datasets/csv/Suicides in india 2001-2012.csv")
      .as[suicides]

    suicidesIndia.show(20)

    println("Grouping based on Year with Filter")

    suicidesIndia
      .distinct()
      .groupBy("Year", "Type")
      .count()
      .orderBy("Type")
      .show(20)

    println("suicides in India 2001 with Filter")
    val suicidesIn2001F = suicidesIndia.filter(suicide => suicide.Year == "2001")
    suicidesIn2001F.collect().foreach(println)
    suicidesIn2001F.show(20)
    println("suicides in India 2001 with where")
    val suicidesIn2001W = suicidesIndia.where("Year === 2001")
    suicidesIn2001W.show(20)
    /**
     * IF we use SELECT on top of this Dataset by fetching only few columns, there are chances
     * of the Dataset getting converted to DataFrame. This is because the structure of Dataset for
     * selecting few columns is not defined.
     *
     * We can define it by adding another case class into it as show below.
     */
    val yearNtype12 = suicidesIndia.select("Year", "Type")
    val yearNtype = suicidesIndia.select("Year", "Type").as[yeartype]
    yearNtype.show(20)

  }
}