package com.atguigu.streaming.project.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * Author atguigu
 * Date 2020/3/24 14:37
 */
object MyKafkaUtils {
    
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "bigdata1015",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    /**
     * 根据传入的参数, 返回从kafka得到的流
     * @param ssc
     * @param topics
     * @return
     */
    def getKafkaSteam(ssc: StreamingContext, topics: String*) =
        KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent, // 标配
            Subscribe[String, String](topics.toIterable, kafkaParams)
        ).map(_.value())
}
