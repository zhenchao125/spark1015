package com.atguigu.spark1015.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 9:30
 */
object Aggregate {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Aggregate").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val r = rdd1.aggregate("<")(_ + _, _ + _)
        println(r)
        
        sc.stop()
        
    }
}
