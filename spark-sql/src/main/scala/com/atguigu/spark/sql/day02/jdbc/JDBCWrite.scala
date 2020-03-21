package com.atguigu.spark.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCWrite {
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "aaaaaa"
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCWrite")
            .getOrCreate()
        
        
        val df = spark.read.json("c:/users.json")
        
        // 写到jdbc中
        /*df.write
            .format("jdbc")
            .option("url", url)
            .option("user", user)
            .option("password", pw)
            .option("dbtable", "usre1015")
//            .mode("append")
            .mode(SaveMode.Overwrite)
            .save()*/
        
        val props = new Properties()
        props.put("user", user)
        props.put("password", pw)
        df.write
            .jdbc(url, "user1016", props)
        
        spark.close()
        
        
    }
}
