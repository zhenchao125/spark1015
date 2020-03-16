package com.atguigu.spark1015.day04.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 15:27
 */
object Persist2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Persist2").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 30, 40, 40)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1.map(x => {
            println("map: " + x)
            (x, 1)
        })
        
        val rdd3 = rdd2.reduceByKey((x, y) => {
            println("reduceByKey: " + (x, y))
            x + y
        })
        rdd3.cache()
        val aa = rdd3.collect()
        println("----分割线----")
        val bb = rdd3.collect()
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}
