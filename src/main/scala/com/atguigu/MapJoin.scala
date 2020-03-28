package com.atguigu

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/28 11:20
 */
object MapJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello")).map((_, 1))
        val rdd2 = sc.parallelize(Array("hello", "atguigu", "world")).map((_, 1))
        
        // map join: 小的广播出去, 大的做map, 在map中完成"join"的操作
        val bd = sc.broadcast(rdd2.collect())
        
        val result = rdd1.flatMap {
            case (k, v) =>
                /*
                (hello,(1,1))
                (hello,(1,1))
                (hello,(1,1))
                (world,(1,1))
                 */
                val data: Array[(String, Int)] = bd.value
                data.filter(_._1 == k).map {
                    case (k2, v2) => (k, (v, v2))
                }
            
        }
        
        
        result.collect.foreach(println)
        
        
        sc.stop()
        
    }
}
