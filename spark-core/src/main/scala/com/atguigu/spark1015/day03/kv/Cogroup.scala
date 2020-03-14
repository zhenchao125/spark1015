package com.atguigu.spark1015.day03.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 15:14
 */
object Cogroup {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Cogroup").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((1, 10),(2, 20),(1, 100),(3, 30)),1)
        val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c"), (4, "e")),1)
        val rdd3: RDD[(Int, (Iterable[Int], Iterable[String]))] = rdd1.cogroup(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
