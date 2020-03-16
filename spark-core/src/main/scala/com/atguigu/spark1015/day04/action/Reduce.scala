package com.atguigu.spark1015.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 9:25
 */
object Reduce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
//        val a: Int = rdd1.reduce(_ + _)
        val a = rdd1.fold(0)(_ + _)
        println(a)
        
        sc.stop()
        
    }
}
/*
reduce是一个行动算子
    和scala的运算几乎一样
 */