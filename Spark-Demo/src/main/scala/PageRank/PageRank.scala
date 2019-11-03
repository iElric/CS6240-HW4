package PageRank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object PageRank {
  // refer to http://www.ccs.neu.edu/home/mirek/code/SparkTC.scala

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nPageRank <k value> <output dir>")
      System.exit(1)
    }

    // Set up for make local

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    // use the first argument as iteration number
    val k = args(0).toInt
    val vertex_number = k * k

    // use ListBuffer, it's like LinkedList in Java
    // create rank list
    val rank_list = new ListBuffer[(Int, Double)]()
    // first append the dummy page
    rank_list.append((0, 0.0))
    // append
    for (i <- 1 to vertex_number) {
      rank_list.append((i, 1.0 / vertex_number))
    }

    // create graph list
    val graph_list = new ListBuffer[(Int, Int)]()
    for (i <- 1 to vertex_number) {
      if (i % k == 0) {
        // dangling page
        graph_list.append((i, 0))
      } else {
        // normal page edge
        graph_list.append((i, i + 1))
      }
    }

    // Parallelized collection tp create rdd
    var rank: RDD[(Int, Double)] = sc.parallelize(rank_list.toList)
    var graph: RDD[(Int, Int)] = sc.parallelize(graph_list.toList)

    var graph_rank = sc.emptyRDD[(Int, Double)]
    // set number of iterations here
    var iteration_number = 1

    for (i <- 1 to iteration_number) {
      graph_rank = graph.join(rank).map(line => (line._2._1, line._2._2))
      var temp_rank = graph_rank.reduceByKey(_+_)
      // lookup will return a list of values, get the first since there should be only one value
      val dummy_rank = temp_rank.lookup(0).head / vertex_number


    }


  }

}
