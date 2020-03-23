package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 15:12
 */
object WordCount2 {
    def createSSC() = {
    
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 把offset的跟踪在checkpoint中
        ssc.checkpoint("ck1")
    
        val params = Map[String, String](
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            "group.id" -> "1015")
        KafkaUtils
            .createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc,
                params,
                Set("first1015")
            )
            .flatMap {
                case (_, v) =>
                    v.split("\\W+")
            }
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()
    
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        /*
        从checkpoint中恢复一个StreamingContext,
        如果checkpoint不存在, 则调用后面的函数去创建一个StreamingContext
         */
        val ssc = StreamingContext.getActiveOrCreate("ck1", createSSC)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
