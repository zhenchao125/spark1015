package com.atguigu.spark1015.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 13:43
 */
object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        // 每key的和
        /*val result = rdd.combineByKey(
            v => v,
            (c: Int, v: Int) => c + v,
            (c1: Int, c2: Int) => c1 + c2
        )*/
        // 每个key在每个分区内的最大值, 然后再求出这些最大值的和
        /*val result = rdd.combineByKey(
            v => v,
            (max: Int, v: Int) => max.max(v),
            (max1: Int, max2: Int) => max1 + max2
        )*/
       /* val result = rdd.combineByKey(
            +_,
            (_: Int).max(_: Int),
            (_: Int) + (_: Int)
        )*/
        
        // 每个key的平均值
        val result = rdd.combineByKey(
            (v:Int) => (v, 1),
            (sumCount: (Int, Int), v) => (sumCount._1 + v, sumCount._2 + 1),
            (sumCount1: (Int, Int), sumCount2: (Int, Int)) => (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2)
        ).mapValues{
            case (sum, count) => sum.toDouble / count
        }
        result.collect.foreach(println)
        
        sc.stop()
        
    }
}

/*
reduceByKey
    分区内聚合和分区间的聚合逻辑一样
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
foldByKey
    分区内聚合和分区间的聚合逻辑一样, 多了一个零值
    零值只在分区内聚合的时候使用
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)
aggregateByKey
    分区内聚合和分区间的聚合逻辑不一样
    也有零值, 也是在分区内聚合使用
    零值是写死
    aggregateByKey(init)(...)
    
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)
combineByKey
    分区内聚合和分区间的聚合逻辑不一样
    零值不是写死的, 零值是根据碰到的每个key的一个value来动态生成
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)





*/
