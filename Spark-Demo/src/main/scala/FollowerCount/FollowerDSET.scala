package FollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FollowerDSET {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nFollowerCount.FollowerDEST <input dir> <output dir>")
      System.exit(1)
    }



    // https://jhui.github.io/2017/01/15/Apache-Spark/
    // The entry point to programming Spark with the Dataset and DataFrame API.
    // run in terminal, not work in intellij
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("FollowerDSET")
      .getOrCreate()

    // import the toDS and toDF
    import sparkSession.implicits._

    // Read the input text file from input dir
    val dataSet: RDD[String] = sparkSession.sparkContext.textFile(args(0))

    /*val counts = dataSet.map(line => (line.split(",")(1), 1))
      .toDF("userId", "count")
      .groupBy("userId")
      .sum("count")
      .orderBy(desc("sum(count)"))*/
    val counts = dataSet.map(line => (line.split(",")(1), 1))
      .toDS()
      .groupBy("_1")
      .sum("_2")
      .orderBy(desc("sum(_2)"))

    counts.write.csv(args(1))
    println(counts.explain(extended = true))


  }
};

