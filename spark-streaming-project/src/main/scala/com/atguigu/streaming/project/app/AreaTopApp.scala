package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/3/24 14:25
 */
object AreaTopApp extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        val dayAreaGrouped = adsInfoStream
            .map(info => ((info.dayString, info.area, info.adsId), 1))
            // 1.先计算每天每地区每广告的点击量
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
            // 2.map出来 (day,area)作为key
            .map {
                case ((day, area, ads), count) => ((day, area), (ads, count))
            }
            .groupByKey()
        
        // 3 4. 每组内进行排序取前3
        val result: DStream[((String, String), List[(String, Int)])] = dayAreaGrouped.map {
            case (key, it: Iterable[(String, Int)]) =>
                (key, it.toList.sortBy(-_._2).take(3))
        }
        // 5. 把数据写入到redis
        /*result.foreachRDD(rdd => {
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
                        import org.json4s.JsonDSL._
                        // 专门用于把集合转成字符串(样例类不行)
                        val value = JsonMethods.compact(JsonMethods.render(adsCountList))
                        client.hset(key, field, value)
                }
                // 3. 关闭到redis的连接
                client.close()  // 其实是把这个客户端还给连接池
            })
        })*/
        import com.atguigu.streaming.project.util.RealUtil._
        result.saveToRedis
    }
}

/*
每天每地区热门广告 Top3

1. 先计算每天每地区每广告的点击量
    ((day,area,ads), 1) => updateStateByKey

2. 按照每天每地区分组

3. 每组内排序, 取前3

5. 把数据写入到redis

数据类型:
    k-v 形式数据库(nosql 数据)
    K:  都是字符串
    V的数据类型:
        5大数据类型
         1. string
         2. set 不重复
         3. list 允许重复
         4. hash map, 存的是field-value
         5. zset
----
((2020-03-24,华中),List((3,14), (1,12), (2,8)))
((2020-03-24,华东),List((2,38), (4,33), (5,32)))
((2020-03-24,华南),List((4,37), (1,36), (5,29)))
((2020-03-24,华北),List((4,41), (3,34), (1,34)))
-----
选择什么类型的数据:
每天一个key
key                                     value
"area:ads:count" + day                  hash
                                        field       value
                                        area        json字符串
                                        "华中"      {3: 14, 1:12, 2:8}

 */
