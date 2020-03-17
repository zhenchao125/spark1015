package com.atguigu.spark1015.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 14:29
 */
object Text {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Text").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.textFile("C:\\0508")
        rdd.collect()
        sc.stop()
        
    }
}
