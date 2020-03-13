package com.atguigu.spark1015.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 10:37
 */
object MapPartitionsDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapPartitionsDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
//        val rdd2 = rdd1.mapPartitions((it: Iterator[Int]) => it.map(_ * 2))
        val rdd2 = rdd1.mapPartitions((it: Iterator[Int]) => {
            println("abc")
//            it.toList // 把分区的内的数据全部加载到内存中, 有可能会导致分区oom
            it.map(_ * 3)
        })
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
/*
map
    传递的函数每个元素执行一次

mapPartitions
    传递的函数, 每个分区执行一次
    
    有个可能会出现oom
 */