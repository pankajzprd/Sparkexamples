package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object RDDTransformations_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RDD transformations and actions")
      .master("local")
      .getOrCreate()

    val statenames = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/datasets/csv/StateNames.csv")
      .rdd

    println("Schema By Spark")
    statenames.take(10).foreach(println)

    val hellordd = spark.sparkContext.textFile("src/test/resources/datasets/sample.txt", 2)
    val hellordd1 = spark.sparkContext.textFile("src/test/resources/datasets/sample1.txt", 4)
    val hellordd2 = spark.sparkContext.textFile("src/test/resources/datasets/idnames.txt")

    val top = hellordd.top(10)
    val top1 = hellordd1.top(10)
    val top2 = hellordd2.top(10)

    println("Map function")

    val mapfunc = top.map(x => (x, x.length()))
    mapfunc.foreach(println)

    println("Flatmap function")

    val flatmapfunc = top.flatMap(line => line.split(" "))
    val mapflat = flatmapfunc.map(x => (x, x.length()))
    flatmapfunc.foreach(println)
    mapflat.foreach(println)

    println("Filter Function")

    val filterfunc = flatmapfunc.filter(value => value == "are")
    filterfunc.foreach(println)

    println("Union function")

    val unionfunc = top.union(top1)
    unionfunc.foreach(println)

    println("Intersect function")
    val intersectfunc = top.intersect(top1)
    intersectfunc.foreach(println)

    println("Distinct function")
    val distinctfunc = top.distinct
    distinctfunc.foreach(println)

    println("GroupbyKey function")
    val groupdata = spark.sparkContext
      .parallelize(Array(('k', 5), ('s', 3), ('s', 4), ('p', 7), ('p', 5), ('t', 8), ('k', 6)), 3)
    val group = groupdata.groupByKey().collect()

    println("ReduceByKey function")

    val words = Array("one", "two", "two", "four", "five", "six", "six", "eight", "nine", "ten")
    val reducedata = spark.sparkContext.parallelize(words).map(w => (w, 1)).reduceByKey(_ + _)
    reducedata.foreach(println)

    println("SortByKey function")
    val sortdata = spark.sparkContext
      .parallelize(Array(('k', 5), ('s', 3), ('s', 4), ('p', 7), ('p', 5), ('t', 8), ('k', 6)), 3)
    val sort = sortdata.sortByKey()
    sort.foreach(println)

    println("Join function")
    val joindata = spark.sparkContext.parallelize(Array(('A', 1), ('b', 2), ('c', 3)))
    val joindata1 = spark.sparkContext.parallelize(Array(('A', 4), ('A', 6), ('b', 7), ('c', 3), ('c', 8)))
    val joinresult = joindata.join(joindata1)
    println(joinresult.collect().mkString(","))

    println("Coalesce function")
    val coalesce = hellordd.coalesce(3, true)
    coalesce.foreach(println)
  }
}