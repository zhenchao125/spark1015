package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/20 14:08
 */
object CreateDS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("CreateDS")
            .getOrCreate()
        import spark.implicits._
        
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        // 把集合转成ds
        val ds: Dataset[Int] = list1.toDS()
        // df能用的ds一定可以用
        ds.show*/
        
        val list = List(User("zs", 10), User("lisi", 20), User("ww", 15))
        val ds= list.toDS()
        // 在ds做sql查询
        ds.createOrReplaceTempView("user")
        spark.sql("select * from user where age > 15").show
        spark.stop()
        
        
        
        spark.close()
        
        
        
    }
}
