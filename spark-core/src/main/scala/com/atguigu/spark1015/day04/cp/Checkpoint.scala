package com.atguigu.spark1015.day04.cp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 15:37
 */
object Checkpoint {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Persit").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
        val list1 = List(30)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val rdd2 = rdd1.map(x => {
            println("map: " + x)
            x + ": " + System.currentTimeMillis()
        })
        val rdd3 = rdd2.filter(x => {
            println("filter: " + x)
            true
        })
    
        //        rdd3.persist(StorageLevel.MEMORY_ONLY)  // 做了一个持久化的计划, 当第一个行动算子执行完毕之后, 就会对这个rdd做持久化
        //        rdd3.persist()
        
        rdd3.checkpoint()
        rdd3.cache()
        rdd3.collect.foreach(println)
        println("---华丽的分割线---")
        rdd3.collect.foreach(println)
        println("---华丽的分割线---")
        rdd3.collect.foreach(println)
        println("---华丽的分割线---")
        rdd3.collect.foreach(println)
    
        Thread.sleep(1000000)
        sc.stop()
    }
}
/*
checkpoint:
    检查点
    他的功能和持久化一致.
    表现是不一样的.
    1. checkpoint, 需要手动指定存储目录
    2. checkpoint的时候, 当第一个job执行完之后, spark内部会立即再起一个job, 专门的去做checkpoint
        持久后会使用第一个job的结果进行持久化
    3. checkpoint 会切断他的血缘关系.
        持久化不会切断血缘关系
    
 */
