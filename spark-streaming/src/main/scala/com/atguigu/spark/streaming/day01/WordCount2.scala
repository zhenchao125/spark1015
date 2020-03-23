package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/3/23 10:26
 */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        // 从RDD队列中读取数据, 仅仅用于做压力测试
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(3))
        val rdds = mutable.Queue[RDD[Int]]()
        val sourceStream = ssc.queueStream(rdds, false)
        val result = sourceStream.reduce(_ + _)
        result.print()
        ssc.start()
        
        val sc = ssc.sparkContext
        while (true) {
            rdds.enqueue(sc.parallelize(1 to 100))
            Thread.sleep(100)
        }
        
        ssc.awaitTermination()
    }
}
