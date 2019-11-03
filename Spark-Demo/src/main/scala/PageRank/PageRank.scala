package PageRank

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val graph: RDD[(Int, Int)] = sc.parallelize(graph_list.toList)

    var graph_rank = sc.emptyRDD[(Int, Double)]
    // set number of iterations here
    val iteration_number = 10

    for (i <- 1 to iteration_number) {
      graph_rank = graph.join(rank).map(tuple => (tuple._2._1, tuple._2._2))
      //graph_rank.collect().foreach(println)
      //actually only reduce for key 0, other key only has one input
      var temp_rank = graph_rank.reduceByKey(_ + _)
      // lookup will return a list of values, get the first since there should be only one value
      val dummy_rank = temp_rank.lookup(0).head / vertex_number
      // update the rank for those who have input from other vertex
      temp_rank = temp_rank.map(tuple => {
        // make the dummy 0 again
        if (tuple._1 == 0) {
          (tuple._1, 0)
        } else {
          (tuple._1, 0.15 * 1.0 / vertex_number + 0.85 * (tuple._2 + dummy_rank))
        }
      })

      //  now we update the rank for those who only have input from dangling vertex

      // for easy look up
      val dict: collection.Map[Int, Double] = temp_rank.collectAsMap()

      // final rank
      rank = rank.map(tuple => {
        if (dict.contains(tuple._1)) {
          (tuple._1, dict.get(tuple._1).head)
        } else {
          // update the vertex who only have input from dangling vertex
          (tuple._1, 0.15 * 1.0 / vertex_number + 0.85 * dummy_rank)
        }
      })
    }

    println(graph_rank.toDebugString)
    // get first 100 as required
    rank.filter(tuple => {tuple._1 <= 100})saveAsTextFile(args(1))

  }

}
