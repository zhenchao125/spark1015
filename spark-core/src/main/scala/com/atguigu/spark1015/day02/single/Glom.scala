package com.atguigu.spark1015.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 14:08
 */
object Glom {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
//        val rdd2 = rdd1.glom().map(x => x.toList)
        val rdd2 = rdd1.glom().map(_.toList)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
