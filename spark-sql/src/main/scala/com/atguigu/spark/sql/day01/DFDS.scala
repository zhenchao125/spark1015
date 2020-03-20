package com.atguigu.spark.sql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author atguigu
 * Date 2020/3/20 14:36
 */
case class People(name: String, age: Long)

object DFDS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DFDS")
            .getOrCreate()
        import spark.implicits._
        
        val df: DataFrame = spark.read.json("c:/users.json")
        // 先有一个样例类
        val ds = df.as[People]
        //        ds.show()
        val df1: DataFrame = ds.toDF()
        df1.show
        spark.close()
        
        
    }
}
