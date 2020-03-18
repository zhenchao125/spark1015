package com.atguigu.spark1015.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object MyAcc {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyAcc").setMaster("local[4]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20,10,30,40,50)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val acc = new MapAcc
        sc.register(acc)
        rdd1.foreach(x => acc.add(x))
        
        println(acc.value)
        
        sc.stop()
        
    }
}

// 将来累加器的值同时包含 sum, count, avg
// (sum, count, avg)
// Map("sum"-> 1000, "count"-> 10, "avg" -> 100)
class MapAcc extends AccumulatorV2[Double, Map[String, Any]] {
    private var map = Map[String, Any]()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
        println("copy...")
        val acc = new MapAcc
        acc.map = map
        acc
    }
    
    // 不可变集合, 直接赋值一个空的集合
    override def reset(): Unit = map = {
        println("reset...")
        Map[String, Any]()
    }
    
    override def add(v: Double): Unit = {
        // 对sum和count进行累加. avg在最后value函数进行计算
        map += "sum" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] + v)
        map += "count" -> (map.getOrElse("count", 0L).asInstanceOf[Long] + 1L)
    }
    
    override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
        // 合并两个map
        other match {
            case o: MapAcc =>
                map +=
                    "sum" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] + o.map.getOrElse("sum", 0D).asInstanceOf[Double])
                map +=
                    "count" -> (map.getOrElse("count", 0L).asInstanceOf[Long] + o.map.getOrElse("count", 0L).asInstanceOf[Long])
            case _ => throw new UnsupportedOperationException
        }
    }
    
    
    override def value: Map[String, Any] = {
        map += "avg" -> (map.getOrElse("sum", 0D).asInstanceOf[Double] / map.getOrElse("count", 0L).asInstanceOf[Long])
        map
    }
}

