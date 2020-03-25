package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/3/25 9:03
 */
object LastHourApp extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        adsInfoStream
            // 1. 先把窗口分好
            .window(Minutes(60), Seconds(3))
            // 2. 按照广告分钟 进行聚合
            .map(info => (info.adsId, info.hmString) -> 1)
            .reduceByKey(_ + _)
            // 3. 再按照广告分组, 把这个广告下所有的分钟记录放在一起
            .map {
                case ((ads, hm), count) => (ads, (hm, count))
            }
            .groupByKey()
            // 4. 写入到redis中
            .foreachRDD(rdd => {
                rdd.foreachPartition((it: Iterator[(String, Iterable[(String, Int)])]) => {
                    if (it.nonEmpty) { // 只是判断是否有下一个元素, 指针不会跳过这元素
                        // 1. 先建立到redis连接
                        val client: Jedis = RedisUtil.getClient
                        // 2. 写元素到redis
                        // 2.1 一个一个的写(昨天)
                        // 2.2 批次写入
                        import org.json4s.JsonDSL._
                        
                        val key = "last:ads:hour:count"
                        val map = it.toMap.map {
                            case (adsId, it) => (adsId, JsonMethods.compact(JsonMethods.render(it)))
//                            case (adsId, it) => (adsId, JsonMethods.compact(JsonMethods.render(it.toMap)))
                        }
                        // scala集合转换成java集合
                        import scala.collection.JavaConversions._
                        println(map)
                        client.hmset(key, map)
                        // 3. 关闭redis (用的是连接池, 实际是把连接归还给连接池)
                        client.close()
                    }
                })
                
            })
        
    }
}

/*
统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量, 每6秒统计一次
1. 各广告, 每分钟               ->            按照 (广告, 分钟) 分组
2. 最近1小时,每6秒统计一次  ->          窗口: 窗口长度1小时  窗口的滑动步长 5s
 
----

1. 先把窗口分好
2. 按照广告分钟 进行聚合
3. 再按照广告分组, 把这个广告下所有的分钟记录放在一起
4. 把结果写在redis中


-------

写到redis'的时候的数据的类型:
1.
    key                             value
    广告id                          json字符串每分钟的点击

2.
key                                          value
"last:ads:hour:count"                        hash
                                             field              value
                                             adsId              json字符串
                                             "1"                {"09:24": 100, "09:25": 110, ...}

 */