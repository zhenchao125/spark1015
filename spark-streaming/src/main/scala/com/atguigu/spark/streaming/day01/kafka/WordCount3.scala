package com.atguigu.spark.streaming.day01.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 15:50
 */
object WordCount3 {
    val groupId = "1015"
    val params = Map[String, String](
        
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "group.id" -> groupId)
    val topics = Set("first1015")
    // KafkaUtils   KafkaCluster
    val cluster: KafkaCluster = new KafkaCluster(params)
    
    /**
     * 读取开始的offsets
     *
     * @return
     */
    def readOffsets() = {
        // 最终返回的map
        var resultMap = Map[TopicAndPartition, Long]()
        // 1. 获取这些topic的所有分区
        val topicAndPartitionSetEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
        topicAndPartitionSetEither match {
            // 2. 获取topic和分区的信息
            case Right(topicAndPartitionSet) =>
                // 3. 获取到分区信息和他的offset
                val topicAndPartitionToLongEither: Either[Err, Map[TopicAndPartition, Long]] =
                    cluster.getConsumerOffsets(groupId, topicAndPartitionSet)
                topicAndPartitionToLongEither match {
                    // 没有每个topic的每个分区都已经存储过偏移量, 表示曾经消费购, 而且也维护过这个偏移量
                    case Right(map) =>
                        resultMap ++= map
                    // 表示这个topic的这个分区是第一次消费
                    case _ =>
                        
                        topicAndPartitionSet.foreach(topicAndPartition => {
                            resultMap += topicAndPartition -> 0L
                        })
                }
            
            case _ => // 表示不存在任何topic
            
        }
        resultMap
    }
    
    def saveOffsets(stream: InputDStream[String]) = {
        // 保存offset一定从kafka消费到的直接的那个Steram保存
        // 每个批次执行一次传递过去的函数
        stream.foreachRDD(rdd => {
            var map = Map[TopicAndPartition, Long]()
            // 如果这个rdd是直接来自于Kafka, 则可以强转成 HasOffsetRanges
            // 这类型就包含了, 这次消费的offsets的信息
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            // 所有的分区的偏移量
            val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            ranges.foreach(OffsetRange => {
                val key = OffsetRange.topicAndPartition()
                val value = OffsetRange.untilOffset
                map += key -> value
            })
            cluster.setConsumerOffsets(groupId, map)
        })
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount3")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        
        val sourceStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            params,
            readOffsets(),
            (handler: MessageAndMetadata[String, String]) => handler.message()
        )
        sourceStream
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print(1000)
        
        
        saveOffsets(sourceStream)
        ssc.start()
        ssc.awaitTermination()
    }
}
