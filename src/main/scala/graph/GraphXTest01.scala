package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date:2022/12/2 19:42
 * author: qyl
 */
object GraphXTest01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cloud").setMaster("local[*]").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val points = sc.makeRDD(Seq(
      (1L, ("zhangsan", 22)),
      (2L, ("list", 23)),
      (6L, ("wangwu", 24)),
      (9L, ("zhaoliu", 25)),
      (133L, ("tianqi", 26)),
      (16L, ("liba", 27)),
      (21L, ("liujiu", 28)),
      (44L, ("wangshi", 29)),
      (158L, ("leo", 30)),
      (5L, ("Javk", 31)),
      (7L, ("tom", 32)),
    ))

    val edges = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    val graph = Graph(points, edges)
    val vertices = graph.connectedComponents().vertices
    //    vertices.foreach(println)
    vertices.join(points).foreach(println)
    vertices.join(points).map({
      case (userid, (cid, (name, age))) => (cid, List((name, age)))
    }).reduceByKey(_ ++ _).foreach(println)
  }
}
