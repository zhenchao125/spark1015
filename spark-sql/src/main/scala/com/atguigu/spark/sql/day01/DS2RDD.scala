package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/20 14:20
 */

object DS2RDD {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DS2RDD")
            .getOrCreate()
        import spark.implicits._
        
        val ds = Seq(User("lisi", 40), User("zs", 20)).toDS
        val rdd: RDD[User] = ds.rdd
        rdd.collect.foreach(println)
        spark.stop()
    }
}
