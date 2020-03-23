package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 9:30
 */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        // 1. 创建StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 从数据源创建一个流:  socket, rdd队列, 自定义接收器,    kafka(重点)
        val sourceStream = ssc.socketTextStream("hadoop102", 9999)
        // 3. 对流做各种转换
        val result = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 4. 行动算子 print  foreach foreachRDD
        result.print()  // 把结果打印在控制台
        // 5. 启动流
        ssc.start()
        // 6. 阻止主线程退出(阻塞主线程)
        ssc.awaitTermination()
    }
}
