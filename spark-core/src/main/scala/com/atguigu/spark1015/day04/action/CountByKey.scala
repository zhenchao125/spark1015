package com.atguigu.spark1015.day04.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 9:18
 */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
//        val wordCount = rdd1.map((_, 1)).reduceByKey(_ + _).collect()
        val wordCount = rdd1.map((_, null)).countByKey()
        println(wordCount)
        
    }
}
