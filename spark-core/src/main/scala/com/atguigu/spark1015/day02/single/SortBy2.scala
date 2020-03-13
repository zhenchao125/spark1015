package com.atguigu.spark1015.day02.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/13 15:42
 */
class Person(val age: Int, val name: String) extends Serializable{
    override def toString: String = s"$age"
}

object SortBy2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(new Person(10, "lisi") :: new  Person(20, "zs") :: new Person(15, "ww") :: Nil)
        
        implicit val ord: Ordering[Person] = new Ordering[Person]{
            override def compare(x: Person, y: Person): Int = x.age - y.age
        }
        // 如果是样例类, 或者元组, ClassTag不需要穿
        val rdd2: RDD[Person] = rdd1.sortBy(x => x)
        rdd2.collect.foreach(println)
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}
