package com.atguigu.spark1015.day03.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 9:14
 */
object PartitionBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        //
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        /*println(rdd2.partitioner)
        val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
        println(rdd3.partitioner)*/
        // 如果按照value来分区
        
        val rdd3 = rdd2.map {
            case (k, v) => (v, k)
        }.partitionBy(new HashPartitioner(5))
            .map {
                case (k, v) => (v, k)
            }
        
        
        rdd3.glom().collect().map(_.toList).foreach(println)
        sc.stop()
    }
}

/*
stage
    表示我们spark程序执行的阶段
        一个阶段内都是并行执行,互相不依靠
       
       一旦碰到shuffle, 会产生新的阶段, 新的阶段一定要等上个阶段执行完毕才能开始运算
 */