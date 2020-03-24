package com.atguigu.spark.streaming.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/24 10:25
 */
object Window1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Window1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        ssc.checkpoint("ck2")
        ssc
            .socketTextStream("hadoop102", 9999)
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            // 每6秒计算一次最近9秒内的wordCount
//            .reduceByKeyAndWindow(_ + _, Seconds(9), slideDuration = Seconds(6))
            .reduceByKeyAndWindow(_ + _, _ - _ , Seconds(9), slideDuration = Seconds(6), filterFunc = _._2 > 0)
            .print()
        ssc.start()
        ssc.awaitTermination()
        
    }
}
