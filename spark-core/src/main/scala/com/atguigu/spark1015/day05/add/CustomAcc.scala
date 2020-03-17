package com.atguigu.spark1015.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 13:32
 */
object CustomAcc {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        // 先注册自定义的累加器
        val acc = new MyIntAcc
        sc.register(acc, "first")
        
        val rdd2: RDD[Int] = rdd1.map(x => {
            acc.add(1)
            x
        })
        rdd2.collect
        println(acc.value)
        sc.stop()
    }
}

// 1. 对什么值进行累加 2. 累加器最终的值
class MyIntAcc extends AccumulatorV2[Int, Int] {
    private var sum = 0
    
    // 判"零", 对缓冲区值进行判"零"
    override def isZero: Boolean = sum == 0
    
    // 把当前的累加复制为一个新的累加器
    override def copy(): AccumulatorV2[Int, Int] = {
        val acc = new MyIntAcc
        acc.sum = sum
        acc
    }
    
    // 重置累加器(就是把缓冲区的值重置为"零")
    override def reset(): Unit = sum = 0
    
    
    // 真正的累加方法(分区内的累加)
    override def add(v: Int): Unit = sum += v
    
    // 分区间的合并  把other的sum合并到this的sum中
    override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
        case acc: MyIntAcc => this.sum += acc.sum
        case _ => this.sum += 0
    }
    
    // 返回累加后的最终值
    override def value: Int = sum
}
