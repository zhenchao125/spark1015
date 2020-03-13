package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 10:11
 */
object CreateRDD {
    def main(args: Array[String]): Unit = {
        // 1. 得到SparkContext
        val conf = new SparkConf().setMaster("local[3]").setAppName("CreateRDD")
        val sc = new SparkContext(conf)
        // 2. 创建RDD  从scala集合得到RDD
        val arr1 = Array(30, 50, 70, 60, 10, 20)
//        val rdd: RDD[Int] = sc.parallelize(arr1)
        val rdd: RDD[Int] = sc.makeRDD(arr1)
        // 3. 转换
        
        // 4. 行动算子
        val arr: Array[Int] = rdd.collect()
        arr.foreach(println)
        // 5. 关闭SparkContext
        sc.stop()
        
    }
}

/*
得到RDD:
    1. 从数据源
        a: 外部数据源
            文件, 数据库, hive...
        b: 从scala集合得到
            带序列的集合都可以得到RDD
    
    
    2. 从其他的RDD转换得到

 */