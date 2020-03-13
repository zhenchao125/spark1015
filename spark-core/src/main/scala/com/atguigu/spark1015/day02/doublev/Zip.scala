package com.atguigu.spark1015.day02.doublev

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 16:39
 */
object Zip {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Double1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 30, 40, 60) // m
        val list2 = List(30, 5, 7, 60, 1, 2) // n
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = sc.parallelize(list2, 2)
        // 拉链: 1. 对应的分区的元素的个数应该一样  2. 分区数也要一样
        // 总结: 1. 总的元素个数相等 2. 分区数相等
        //        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
        
        /*val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
            it1.zip(it2)
        })*/
        
        // 1. 分区数据必须相等
        /*val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
            it1.zipAll(it2, 100, 200)
        })*/
        
        val rdd3 = rdd1.zipWithIndex()
        rdd3.collect.foreach(println)
        
        sc.stop()
        
    }
}
