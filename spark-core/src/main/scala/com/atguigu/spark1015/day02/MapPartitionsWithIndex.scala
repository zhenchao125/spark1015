package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndex {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        // (30, 0), (50, 0)...
        val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => {
            it.map(x => (index, x))
            //            it.map((index, _))
        })
    
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
