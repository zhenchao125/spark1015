package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 14:29
 */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
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
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
kafkaUtils
 */