package graph

import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.RedisClient

import java.util.concurrent.{Executors, TimeUnit}
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
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() => dailyUpdate(), 0, 12, TimeUnit.HOURS)
  }

  def dailyUpdate(): Unit = {
    val conf = new SparkConf().setAppName("SecondaryDegreeRelationship").setMaster("local[*]").set("spark.testing.memory", "512000000")
    val sparkContext = SparkContext.getOrCreate(conf)
    val edges: RDD[Edge[ED]] = sparkContext.textFile("../focus.csv").map(line
    => {
      val tokens = line.split(",")
      Edge[ED](tokens(0).toLong, tokens(1).toLong, null)
    })

    // 构造
    val graph = Graph.fromEdges(edges, VD(mutable.Map[Long, Int]()))
    //需要将图进行翻转
    val reversalGraph = graph.reverse

    // 遍历图，同时更新点之间的距离
    val degreeRelation_1 = reversalGraph
      .aggregateMessages[mutable.Map[Long, Int]](triplet => {
        triplet.sendToDst(triplet.srcAttr.map + ((triplet.srcId, 1)))
      }, _ ++ _)

    // 第二遍遍历图，同时更新点之间的距离
    val degreeRelation_2: VertexRDD[mutable.Map[Long, Int]] = reversalGraph
      .outerJoinVertices(degreeRelation_1)((vertexId, oldVD, mapOption)
      => mapOption.getOrElse(mutable.Map[Long, Int]()))
      .aggregateMessages[mutable.Map[Long, Int]](triplet => {
        val message = triplet.srcAttr.map(t => (t._1, t._2 + 1)).+((triplet.srcId, 1))
        triplet.sendToDst(message)
      },
        // 最短距离
        (m1: mutable.Map[Long, Int], m2: mutable.Map[Long, Int]) => {
          m1.foldLeft(m2) { case (m, (k, v)) => m + (k -> Math.min(v, m.getOrElse(k, v))) }
        })
    // 这种不常更新但是需要一直进行查询的数据，存入Redis
    val client = RedisClient.getRedisClient

    val array = graph.outerJoinVertices(degreeRelation_2)((vertexId, oldVD, mapOption)
    => mapOption.getOrElse(mutable.Map[Long, Int]())).vertices
      .collect()

    for (elem <- array) {
      client.set(elem._1.toString, StringUtils.join(elem._2.toString(), ","))
    }
  }
}


