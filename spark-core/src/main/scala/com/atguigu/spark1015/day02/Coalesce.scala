package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 15:17
 */
object Coalesce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 5)
    
        println(rdd1.getNumPartitions)
        // 减少分区的时候, 默认是不会进行shuffle
//        val rdd2 = rdd1.coalesce(10)
        val rdd2 = rdd1.coalesce(2, true)
        println(rdd2.getNumPartitions)
        
        sc.stop()
        
    }
}
/*
coalesce
    一般用来减少分区
    默认只能减少分区
    coalesce(6, true)
        参数2表示是否shuffle, 如果是true, 就可以增加分区
    注意: 如果增加分区就一定要shuffle
         减少分区一般不要shuffle


    


 */