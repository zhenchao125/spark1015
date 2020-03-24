package com.atguigu.streaming.project.util

import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/3/24 16:30
 */
object RealUtil {
    
    implicit class MyRedis(stream:DStream[((String, String), List[(String, Int)])] ){
        
        def saveToRedis = {
            stream.foreachRDD(rdd => {
                rdd.foreachPartition((it: Iterator[((String, String), List[(String, Int)])]) => {
                    // 1. 建立到redis的连接
                    val client: Jedis = RedisUtil.getClient
                    // 2. 写数据到redis
                    it.foreach {
                        // ((2020-03-24,华中),List((3,14), (1,12), (2,8)))
                        case ((day, area), adsCountList) =>
                            val key = "area:ads:count" + day
                            val field = area
                            // 把集合转换成json字符串  json4s
                            // 专门用于把集合转成字符串(样例类不行)
                            import org.json4s.JsonDSL._
                            val value = JsonMethods.compact(JsonMethods.render(adsCountList))
                            client.hset(key, field, value)
                    }
                    // 3. 关闭到redis的连接
                    client.close()  // 其实是把这个客户端还给连接池
                })
            })
        }
    }
    
}
