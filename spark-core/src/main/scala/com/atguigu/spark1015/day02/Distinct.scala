package com.atguigu.spark1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 15:02
 */
case class User(age: Int, name: String){
    override def hashCode(): Int = this.age
    override def equals(obj: Any): Boolean = obj match {
        case User(age, _) => this.age == age
        case _ => false
    }
}
object Distinct {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
       /* val list1 = List(30, 50, 70, 60, 10, 20, 10, 30, 50, 70)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = rdd1.distinct()*/
        
        val rdd1 = sc.parallelize(List(User(10, "lisi"), User(20, "zs"), User(10, "ab")))
//        val rdd2: RDD[User] = rdd1.distinct()
        
        val rdd2 = rdd1.distinct(2)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
