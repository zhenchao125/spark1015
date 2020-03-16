package com.atguigu.spark1015.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 9:08
 */
object Action1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Action1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = 1 to 100000
        val rdd1= sc.parallelize(list1, 100)
        val arr = rdd1.map(x =>{Thread.sleep(10) ;x * x}).collect
//        val arr = rdd1.take(10)
        /*val a = rdd1.map(x => {
            println("map...")
            x
        }).filter(x => {
            println("filter...")
            x > 20
        }).count*/
        println(arr.toList)
        sc.stop()
        
    }
}
