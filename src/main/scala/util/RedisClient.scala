package util


import redis.clients.jedis.Jedis

/**
 * @description redis 工具类
 * @date:2022/12/12 16:18
 * @author: qyl
 */
object RedisClient {
  def getRedisClient: Jedis = {
    val port = 6379 // 你的端口号
    val ip = "127.0.0.1" // 你的具体redis的ip地址
    val redisClient = new Jedis(ip, port)
    redisClient
  }
}
