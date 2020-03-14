package com.atguigu.spark1015.day03.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 15:01
 */
object SortByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
        val rdd2: RDD[(Int, String)] = rdd.sortByKey(false)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
/*
sortByKey
    1. 用来排序.
    2. 只能用来排序kv形式的RDD
    3. 这个用的比较少
    4. sortBy 这个用的比较多
    5. 即使有很大量的数据, 也不会出现oom.
       所以, 排序的时候尽量使用spark的排序, 避免使用scala的排序
 */