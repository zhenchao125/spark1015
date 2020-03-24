package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.util.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream: DStream[String] = MyKafkaUtils.getKafkaSteam(ssc, "ads_log1015")
        
        val adsInfoStream: DStream[AdsInfo] = sourceStream.map(s => {
            val spilt: Array[String] = s.split(",")
            AdsInfo(spilt(0).toLong, spilt(1), spilt(2), spilt(3), spilt(4))
        })
        
        doSomething(adsInfoStream)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
    
    def doSomething(adsInfoStream: DStream[AdsInfo]): Unit
    
}
