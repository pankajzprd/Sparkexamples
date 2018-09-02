package sparkcourse.sparkpackage1

import org.apache.spark.sql.SparkSession

object sampletry {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Connecting to Cluster")
      .master("yarn-client")
      /*.config("spark.hadoop.fs.defaultFS", "hdfs://nn01.itversity.com:8020")
      .config("spark.hadoop.yarn.resourcemanager.hostname", "rm01.itversity.com")
      .config("spark.hadoop.yarn.resourcemanager.address", "rm01.itversity.com:8050")*/
      .config("spark.yarn.jars", "hdfs://nn01.itversity.com:8020/user/trainingv/jars/*.jar")
     // .config("spark.hadoop.yarn.application.classpath", "/usr/hdp/2.6.5.0-292/hadoop/conf,/usr/hdp/2.6.5.0-292/hadoop/*,/usr/hdp/2.6.5.0-292/hadoop/lib/*,/usr/hdp/current/hadoop-hdfs-client/*,/usr/hdp/current/hadoop-hdfs-client/lib/*,/usr/hdp/current/hadoop-yarn-client/*,/usr/hdp/current/hadoop-yarn-client/lib/*,/usr/hdp/current/ext/hadoop/*")
      .getOrCreate()

    println("Connected")

  }
}