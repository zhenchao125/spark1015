package com.atguigu.spark.sql.day01

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/20 11:32
 */
case class User(name: String, age: Int)

object RDD2DF {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        val sc: SparkContext = spark.sparkContext
        //        val rdd = sc.parallelize(1 to 10)
        /*val rdd: RDD[(String, Int)] = sc.parallelize(("lisi", 10):: ("zs", 20)::Nil)
        val df = rdd.toDF("name", "age")*/
        
        val rdd = sc.parallelize(Array(User("lisi", 10), User("zs", 20), User("ww", 15)))
        //        rdd.toDF.show
        
        rdd.toDF("n", "a").show
        spark.stop()
    }
}

/*
RDD转换成df
 */