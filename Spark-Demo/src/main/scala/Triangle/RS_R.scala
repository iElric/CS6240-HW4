package Triangle

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RS_R {

  def main(args: Array[String]) {
    // set the filter, change here
    val MAX = 15000
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    // set up local run arguments
    if (args.length != 2) {
      logger.error("Usage:\nTriangle.RS_R <input dir> <output dir>")
      System.exit(1)
    }
    // Set up configuration
    val conf = new SparkConf().setAppName("RS_R")
    val sc = new SparkContext(conf)
    // Read the input text file from input dir
    val textFile = sc.textFile(args(0))

    // use id1 as the key
    val user_first = textFile
      // map to a tuple
      .map(line => (line.split(",")(0), line.split(",")(1)))
      // convert string to int
      .filter(line => line._1.toInt <= MAX && line._2.toInt <= MAX)

    // use id2 as the key
    val user_second = textFile
      .map(line => (line.split(",")(1), line.split(",")(0)))
      .filter(line => line._1.toInt <= MAX && line._2.toInt <= MAX)

    // we compute a path2, tuple join a tuple will result in a format like(41,(126,140))
    // we have to filer the tuple like (96,(41,41))
    val path2 = user_second.join(user_first).filter(line => line._2._1 != line._2._2)
      // use null since we don't care value
      .map(line => ((line._2._2, line._2._1), null))

    val users = user_first.map(line => ((line._1, line._2), null))
    val triangle = path2.join(users)
    // divide by 3 since we calculate each triangle third times
    val triangleCount = triangle.count() / 3
    // save as rdd
    val resultCount = sc.parallelize(Seq("TriangleCount: ", triangleCount))
    // Output the counting file
    resultCount.saveAsTextFile(args(1))
    //println("The count of closed triangles: " + triangleCount)
    //path2.foreach(println)
  }
};