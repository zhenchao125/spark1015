package com.atguigu.spark1015.day05.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 16:08
 */
object BroadCastDemo1 {
    def main(args: Array[String]): Unit = {
        val bigArr = 1 to 1000 toArray
        val conf: SparkConf = new SparkConf().setAppName("BroadCastDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        // 广播出去.
        val bd = sc.broadcast(bigArr)
        
        val list1 = List(30, 50000000, 70, 600000, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        
        val rdd2 = rdd1.filter(x => bd.value.contains(x))
        rdd2.collect.foreach(println)
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}
