package com.atguigu.spark1015.day05.text

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 9:11
 */
object TextFile {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf: SparkConf = new SparkConf().setAppName("TextFile")
            .setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.parallelize("hello world" :: "hello" :: Nil)
            .flatMap(_.split("\\W+")).map((_, 1))
            .reduceByKey(_ + _)
            //
            .saveAsTextFile("/word1017")
        
        sc.stop()
        
    }
}
