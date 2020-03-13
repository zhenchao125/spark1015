package com.atguigu.spark1015.day02.double

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 16:39
 */
object Double1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Double1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 30)  // m
        val list2 = List(30, 5, 7, 60, 1, 2, 30)  // n
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = sc.parallelize(list2, 3)
        // 并集
//        val rdd3: RDD[Int] = rdd1.union(rdd2)
//        val rdd3: RDD[Int] = rdd1 ++ rdd2
        // 交集
//        val rdd3 = rdd1.intersection(rdd2)
        // 差集
//        val rdd3 = rdd1.subtract(rdd2)
        
        // 笛卡尔积
        val rdd3 = rdd1.cartesian(rdd2)  // m * n
        rdd3.collect.foreach(println)
        
        sc.stop()
        
    }
}
