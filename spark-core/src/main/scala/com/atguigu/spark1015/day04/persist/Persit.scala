package com.atguigu.spark1015.day04.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 14:49
 */
object Persit {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Persit").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1.map(x => {
            println("map: " + x)
            x
        })
        val rdd3 = rdd2.filter(x => {
            println("filter: " + x)
            true
        })
        
//        rdd3.persist(StorageLevel.MEMORY_ONLY)  // 做了一个持久化的计划, 当第一个行动算子执行完毕之后, 就会对这个rdd做持久化
//        rdd3.persist()
        rdd2.cache()
        rdd3.collect
        println("---华丽的分割线---")
        rdd3.collect
        println("---华丽的分割线---")
        rdd3.collect
        
        Thread.sleep(100000)
        sc.stop()
        
    }
}
