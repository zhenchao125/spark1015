package com.atguigu.spark1015.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 13:32
 */
object Add1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val acc: LongAccumulator = sc.longAccumulator("one")
        
        val rdd2: RDD[Int] = rdd1.map(x => {
            acc.add(1)
            x
        })
        rdd2.collect
        println(acc.value)
        Thread.sleep(10000000)
        sc.stop()
    }
}
