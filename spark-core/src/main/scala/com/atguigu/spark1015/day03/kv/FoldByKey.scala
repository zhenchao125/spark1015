package com.atguigu.spark1015.day03.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/3/14 10:39
 */
object FoldByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[4]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "hello"))
        val wordOne: RDD[(String, Int)] = rdd1.map((_, 1))
    
        val result: RDD[(String, Int)] = wordOne.foldByKey(1)(_ + _)
        result.collect.foreach(println)
        
        sc.stop()
    }
}
/*
foldByKey
    1. 折叠. 也是聚合, 和reduceByKey一样, 也有聚合.
            所有的聚合算子都有预聚合
    2. 多了一个0值的功能
    3. 0值到底参与了多次运算?
        只在分区内聚合(预聚合的)时有效
    
 */
