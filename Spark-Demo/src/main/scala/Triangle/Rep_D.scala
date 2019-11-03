package Triangle

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Rep_D {

  def main(args: Array[String]) {
    // set the filter, change here
    val MAX = 50000

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    // set up local run arguments
    if (args.length != 2) {
      logger.error("Usage:\nTriangle.Rep_D <input dir> <output dir>")
      System.exit(1)
    }
    // Set up configuration

    val sparkSession = SparkSession.builder
      .appName("Rep-D")
      .getOrCreate()

    import sparkSession.implicits._

    val textFile = sparkSession.sparkContext.textFile(args(0))
    val user = textFile
      .map(line => (line.split(",")(0), line.split(",")(1)))
      .toDF("id1", "id2").filter($"id1" <= MAX && $"id2" <= MAX)

    // force to broadcast
    val bUser = broadcast(user)

    /* +---+---+---+---+
       |id1|id2|id1|id2|
       +---+---+---+---+
       |467|534| 41|467|
       |675| 41|534|675|
       |675|534| 41|675|*/

    val path2 = user.as("first").join(bUser.as("second"))
      .filter($"first.id1" === $"second.id2" && $"first.id2" =!= $"second.id1")
      // rename
      .toDF("_a", "id3", "id1", "_b").drop("_a", "_b")


    val triangle = path2.as("path2").join(bUser.as("user"))
      .filter($"path2.id1" === $"user.id2" && $"path2.id3" === $"user.id1")



    //triangle.show()

    val count = triangle.count() / 3

    val result = sparkSession.sparkContext.parallelize(Seq("TriangleCount: ", count))

    // Output the counting file
    result.saveAsTextFile(args(1))
    println("The count of triangles: " + count)
    path2.explain()
    triangle.explain()
  }
};