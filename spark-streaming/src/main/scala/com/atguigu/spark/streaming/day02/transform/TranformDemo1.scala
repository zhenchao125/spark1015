package com.atguigu.spark.streaming.day02.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/24 9:24
 */
object TranformDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TranformDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        val result = sourceStream.transform(rdd => {
            rdd.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_+ _)
        })
        result.print
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
