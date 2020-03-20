package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/3/20 11:51
 */
object DF2RDD {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DF2RDD")
            .getOrCreate()
        import spark.implicits._
        // 直接从一个scala的集合到df, 做练习或测试用
        /*val df: DataFrame = (1 to 10).toDF("number")
        // 转rdd  rdd中存储的一定是Row
        val rdd = df.rdd
        val rdd1 = rdd.map(row => row.getInt(0))
        rdd1.collect.foreach(println)*/
        val df = spark.read.json("c:/users.json")
        df.printSchema()
        val rdd1 = df.rdd.map(row => {
            User(row.getString(1), row.getLong(0).toInt)
        })
        rdd1.collect.foreach(println)
        
        
        spark.close()
        
        
    }
}
