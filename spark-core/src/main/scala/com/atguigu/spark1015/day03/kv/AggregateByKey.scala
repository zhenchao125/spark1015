package com.atguigu.spark1015.day03.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/3/14 11:13
 */
object AggregateByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
        // 计算每个分区相同key最大值, 然后相加
        //1.   1. 分区内是计算最大值  2. 分区间是求和
//        val result = rdd.aggregateByKey(Int.MinValue)((u, v) => u.max(v) , (u1, u2) => u1 + u2)
        /*val result =
        rdd.aggregateByKey(Int.MinValue)(_.max(_) , _ + _)*/
        
        // 2.  1. 分区内同时计算最大值和最小值  2. 分区间计算最大值的和与最小值的和
        /*val result =
            rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
                {
                    case ((max, min), v) => (max.max(v), min.min(v))
                },
                {
                    case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
                }
            )*/
        
        // 3. 计算每个key的平均值  a:每个key的value和 b: 每个key出现的次数
        val result =
            rdd.aggregateByKey((0, 0))(
                {
                    case ((sum, count), v) => (sum + v, count + 1)
                },
                {
                    case ((sum1, count1), (sum2, count2)) =>(sum1 + sum2, count1 + count2)
                }
            )/*.map{
                case (k, (sum, count)) => (k, sum.toDouble / count)
            }*/.mapValues{
                case (sum,  count) => sum.toDouble / count
            }
        
        result.collect.foreach(println)
        sc.stop()
    }
}
/*
aggregateByKey
    1. reduceByKey, foldByKey 都有预聚合
        分区内聚合的逻辑和分区间聚合的逻辑是一样的
    2. aggregateByKey 实现了分区聚合逻辑和分区间的聚合逻辑不一样
 */
class User{
    self =>
    val a = 10
    def foo(a:Int) = {
        println(a)
        println(this.a)
        println(self.a)  // self就是this
    }
}
