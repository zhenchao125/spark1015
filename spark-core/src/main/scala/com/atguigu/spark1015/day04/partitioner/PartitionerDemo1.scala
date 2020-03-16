package com.atguigu.spark1015.day04.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * Author atguigu
 * Date 2020/3/16 16:10
 */
object PartitionerDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PartitionerDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20/*, null, null, 'a', 'b', 'c'*/)
        val rdd1 = sc.parallelize(list1, 4).map(x => {
            println(x)
            (x, 1)
        })
        
        val rdd2 = rdd1.partitionBy(new MyPartitioner(3))
        rdd2.glom().map(_.toList).collect().foreach(println)
        sc.stop()
        
    }
}

// 自定义的Hash分区器
class MyPartitioner(num: Int) extends Partitioner {
    assert(num > 0)
    
    override def numPartitions: Int = num
    
    override def getPartition(key: Any): Int = key match {
        case null => 0
        case _ => key.hashCode().abs % num
    }
}