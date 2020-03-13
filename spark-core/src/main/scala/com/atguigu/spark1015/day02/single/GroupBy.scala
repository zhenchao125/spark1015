package com.atguigu.spark1015.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 14:13
 */
object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 7, 6, 1, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1.groupBy(x => x % 2)
        val rdd3 = rdd2.map {
            case (k, it) => (k, it.sum)
        }
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
