package com.atguigu.spark.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/21 9:28
 */
object JDBCRead {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCRead")
            .getOrCreate()
        
        
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val user = "root"
        val pw = "aaaaaa"
        
        /*val df = spark.read
            .option("url", url)
            .option("user", user)
            .option("password", pw)
            .option("dbtable", "user")
            .format("jdbc").load()
        
        df.show*/
        
        val props = new Properties()
        props.put("user", user)
        props.put("password", pw)
        
        val df = spark.read
            .jdbc(url, "user", props)
        
        df.show
        spark.close()
        
        
    }
}

/*
从jdbc读数据
    通用
    
    
    专用
 */
