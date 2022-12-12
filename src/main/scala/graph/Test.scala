package graph

import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @description
 * @date:2022/12/12 13:27
 * @author: qyl
 */

object SecondaryDegreeRelationship {
  case class User(name: String, age: Int)

  case class VD(map: mutable.Map[Long, Int])

  case class ED()

  case class Message()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondaryDegreeRelationship").setMaster("local[1]")
    val sparkContext = SparkContext.getOrCreate(conf)
    //      val edges: RDD[Edge[ED]] = sparkContext.textFile(this.getClass.getResource("/test.csv").getPath).map(line
    //      => {
    //        val tokens = line.split(",")
    //        Edge[ED](tokens(0).toLong, tokens(1).toLong, null)
    //      })

    val users = mutable.Map(
      (1L, User("Alice", 20)),
      (2L, User("Jack", 21)),
      (3L, User("Leo", 24)),
      (4L, User("Tom", 25)),
      (5L, User("zhangsan", 50)),
      (6L, User("Lisi", 45))
    )

    val edgeArray = Array(
      Edge(2L, 1L, 0),
      Edge(2L, 4L, 0),
      Edge(3L, 2L, 0),
      Edge(3L, 6L, 0),
      Edge(4L, 1L, 0),
      Edge(5L, 2L, 0),
      Edge(5L, 3L, 0),
      Edge(5L, 6L, 0)
    )
    // 构造
    val edgeRDD = sparkContext.parallelize(edgeArray)
    val graph = Graph.fromEdges(edgeRDD, VD(mutable.Map[Long, Int]()))
    //需要将图进行翻转
    val reversalGraph = graph.reverse
    val degreeRelation_1: VertexRDD[mutable.Map[Long, Int]] = reversalGraph
      .aggregateMessages[mutable.Map[Long, Int]](triplet => {
        triplet.sendToDst(triplet.srcAttr.map + ((triplet.srcId, 1)))
      }, _ ++ _)
    val degreeRelation_2: VertexRDD[mutable.Map[Long, Int]] = reversalGraph
      .outerJoinVertices(degreeRelation_1)((vertexId, oldVD, mapOption)
      => mapOption.getOrElse(mutable.Map[Long, Int]()))
      .aggregateMessages[mutable.Map[Long, Int]](triplet => {
        val message = triplet.srcAttr.map(t => (t._1, t._2 + 1)).+((triplet.srcId, 1))
        triplet.sendToDst(message)
      }, (m1: mutable.Map[Long, Int], m2: mutable.Map[Long, Int]) => {
        (m1.foldLeft(m2)) { case (m, (k, v)) => m + (k -> Math.min(v, m.getOrElse(k, v))) }
      })

    graph.outerJoinVertices(degreeRelation_2)((vertexId, oldVD, mapOption)
    => mapOption.getOrElse(mutable.Map[Long, Int]())).vertices.foreach(v => {
      println(s"推荐 ${users(v._1).name} 关注用户 : ")
      v._2.keys.map(id => users(id).name).foreach(println)
    })
  }
}


