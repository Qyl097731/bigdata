package graph

import org.apache.spark.{SparkConf, SparkContext}
import util.RedisClient

/**
 * @description
 * @date:2022/12/12 20:36
 * @author: qyl
 */
object Recommend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Recommend").setMaster("local[*]").set("spark.testing.memory", "512000000")
    val sparkContext = SparkContext.getOrCreate(conf)
    val users = sparkContext.textFile("../users.csv").map(line => {
      val tokens = line.split(",")
      (tokens(0).toLong, User(tokens(1), tokens(2).toInt))
    }).collectAsMap()

    val client = RedisClient.getRedisClient

    args.foreach(arg => {
      println(s"推荐 ${users(arg.toLong).name} 关注 ：")
      val value = client.get(arg)
      val listStr = value.substring(value.indexOf("(") + 1, value.lastIndexOf(")"));
      listStr.split(",").foreach(item => {
        println(users(item.substring(0, item.indexOf("-")).trim.toLong).name)
      })
    })
  }
}
