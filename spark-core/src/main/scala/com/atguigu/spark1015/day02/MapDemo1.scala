package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 10:28
 */
object MapDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
//        val rdd2 = rdd1.map(x => x * 2)
        val rdd2 = rdd1.map(_ * 2)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}

/*
map
    针对集合中的每个元素执行一次传递的函数
    一进一出
    
    将来用来做数据结构的调整
 */