package Triangle

import java.io

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Rep_R {

  def main(args: Array[String]) {
    val MAX = 50000
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nTriangle.Rep_R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Rep_R")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))


    val edge = textFile
      .map(line => (line.split(",")(0), line.split(",")(1)))
      .filter(line => line._1.toInt <= MAX && line._2.toInt <= MAX)

    // create a tuple (id2, (id1, id2))
    val second = edge.map(line => (line._2, line))

    // create a new hash set, any iterable should work. just like linked list I used in
    // map reduce rep join
    val hashSet = mutable.HashSet.empty[String]
    // function, add an element to the set
    val addToSet = (set: mutable.HashSet[String], element: String) => set += element
    // function, merge two sets
    val mergeSet = (set1: mutable.HashSet[String], set2: mutable.HashSet[String]) => set1 ++= set2
    // broadcast, https://learning.oreilly.com/library/view/high-performance-spark/9781491943199/ch04.html
    val broadcastMap = sc.broadcast(edge
      // really handy, simplify the process to create a iterable as value
      .aggregateByKey(hashSet)(addToSet, mergeSet)
      // now we have a map, id -> set(user_ids which id is following)
      .collectAsMap())

    val path2: RDD[(String, String)] = second.flatMap {
      case (id2, record) => broadcastMap.value.get(id2) match {
        // seq.empty has type but does not hold value
        case None => Seq.empty[(String, (String, String))]
        // Some indicate the option is valid, get the hash set
        // assemble a path2 here, (id1, id3)
        case Some(id) => id.map(id3 => (record._1, id3))
      }
    }.map(line => (line._1, line._2.toString))

    val triangle: RDD[(String, io.Serializable)] = path2.flatMap {
      // try to get the id3 set and examine if id1 in it
      case (id1, id3) => broadcastMap.value.get(id3) match {
        case None => Seq.empty[(String, (String, String))]
          // get each element in the id3 hash set, check if the element is id1
        case Some(id) => id.map(id_1 => (id1, id_1))
          .filter(line => line._1 == line._2)
      }
    }

    val triangleCount = sc.parallelize(
      Seq("TriangleCount: ", triangle.count() / 3))

    // Output the counting file
    triangleCount.saveAsTextFile(args(1))
  }
};
