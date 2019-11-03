package PageRank

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


object PageRank {
  // refer to http://www.ccs.neu.edu/home/mirek/code/SparkTC.scala

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Arguments error: use PageRank with <k value> <output dir>")
      System.exit(1)
    }

    // Set up for make local
    val spark = SparkSession
      .builder()
      .appName("PageRank")
      .master("local")
      .getOrCreate()

    // use the first argument as iteration number
    val k = args(0).toInt
    val vertex_number = k * k

    // use ListBuffer, it's like LinkedList in Java
    


  }

}
