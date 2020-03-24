package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.util.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MyKafkaUtils.getKafkaSteam(ssc, "ads_log1015")
        ssc.start()
        ssc.awaitTermination()
        
    }
    
}
