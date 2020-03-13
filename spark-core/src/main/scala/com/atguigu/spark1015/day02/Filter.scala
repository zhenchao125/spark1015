package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 14:06
 */
object Filter {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Filter").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2 = rdd1.filter(x => x > 20)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
