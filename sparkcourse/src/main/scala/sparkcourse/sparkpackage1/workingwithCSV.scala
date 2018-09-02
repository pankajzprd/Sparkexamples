package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object workingwithCSV {
  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder()
      .appName("Working with CSV files")
      .master("local")
      .getOrCreate()

    val csvRDD = sparksession.sparkContext.textFile("src/test/resources/datasets/csv/zomato.csv", 4)
    val csvRDD1 = sparksession.sparkContext.textFile("file:\\E:\\Downloads\\Datasets\\zomato.csv", 2)
    println("The total number of records are: ", csvRDD1.count())
    csvRDD.take(10).foreach(println);
    csvRDD1.take(10).foreach(println);

    // removing the header from csv file
    val header = csvRDD1.first()
    val csvRDDNoHeader = csvRDD1.filter(line => line != header)
    csvRDDNoHeader.take(10).foreach(println)

    csvRDD1.saveAsTextFile("file:\\E:\\Downloads\\Datasets\\zomato.txt")
    // printing specific contents of csv file
    val csvspecifics = csvRDDNoHeader.map(line => {
      val array = line.split(",")
      Array(array(0), array(1), array(2), array(3), array(4), array(12)).mkString("|")
    }).take(10).foreach(println)

  }
}