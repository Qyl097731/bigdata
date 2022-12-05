package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date:2022/12/2 20:39
 * author: qyl
 */
object GraphXTest02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cloud").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Jack", 27)),
      (3L, ("Leo", 65)),
      (4L, ("Tom", 42)),
      (5L, ("zhangsan", 55)),
      (6L, ("Lisi", 50)),
    )

    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    // 构造
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    val graph = Graph(vertexRDD, edgeRDD)

    /**
     * 图的属性
     */
    println("属性演示")
    println("**************************")
    println("找出图中年龄大于30的顶点")
    graph.vertices.filter({
      case (id, (name, age)) => age > 30
    }).collect().foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    // 边操作 找出图中属性大于5的边
    println("找出图中属性大于5的边")
    graph.edges.filter(e => e.attr > 5).collect().foreach(e => println(s"${e.srcId} to ${e.dstId} is ${e.attr}"))

    println("列出边属性 > 5的Triplets")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} links ${triplet.dstAttr._1}")
    }

    println("找出途中最大的出度、入读、度数")
    println("max of outDegrees : " + graph.outDegrees.max() +
      "max of inDegrees : " + graph.inDegrees.max() +
      "max of Degrees : " + graph.degrees.max())

    println("转换操作****************")
    println("顶点的转换操作，顶点的age+10")
    graph.mapVertices {
      case (id, (name, age)) => (id, (name, age + 10))
    }.vertices.collect.foreach(println)

    println("边的转换操作，边的属性*2")
    graph.mapEdges(e => e.attr * 2).edges.collect.foreach(println)

    /**
     * 结构操作
     */
    println("结构操作")
    println("顶点 > 30 的子图")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)

    println("子图所有顶点")
    subGraph.vertices.collect.foreach(println)

    println("子图所有边")
    subGraph.edges.collect.foreach(println)

    /**
     * 连接操作
     */
    println("连接操作")
    println("***********************************")
    val idDegrees = graph.inDegrees
    // 创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph = graph.mapVertices {
      case (id, (name, age)) => User(name, age, 0, 0)
    }
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOut) => User(u.name, u.age, inDegOut.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegout) => User(u.name, u.age, u.inDeg, outDegout.getOrElse(0))
    }
    println("连接图的属性")
    userGraph.vertices.foreach(println)
    println("出度和入读相同的人员")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach(println)
  }
}

case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

