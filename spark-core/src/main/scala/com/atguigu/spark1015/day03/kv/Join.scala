package com.atguigu.spark1015.day03.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 15:20
 */
object Join {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        var rdd2 = sc.parallelize(Array((1, "aa"), (1, "dd"), (3, "bb"), (2, "cc")))
        // 1. 内连接
//        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        // 2. 左外连接
//        val  rdd3 = rdd1.leftOuterJoin(rdd2)
        // 3. 右外连接
//        val rdd3 = rdd1.rightOuterJoin(rdd2)
        // 4. 全外连接
        val rdd3 = rdd1.fullOuterJoin(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
/*
join
    1. 含义和sql差不多, 用来连接两个RDD
    2. 连接肯定需要连接条件   on a.id=b.id
        rdd1.k=rdd2.k
    3. sql: 内, 左外,右外, 全外
       rdd: 内,...
       
mysql 支持fulljoin吗?
    不支持
    如何实现hive的fulljoin?
    
    left
    union all
    right
     where is left.id=null
 */
