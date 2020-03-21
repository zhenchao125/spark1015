package com.atguigu.spark.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/21 11:35
 */
object HiveWrite {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveWrite")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        // 先创建换一个数据库
//        spark.sql("create database spark1016").show
        /*spark.sql("create table user1(id int, name string)").show
        spark.sql("insert into user1 values(10, 'lisi')").show*/
        
        val df = spark.read.json("c:/users.json")
        spark.sql("use spark1016")
        // 直接把数据写入到hive中. 表可以存着也可以不存在
        df.write.mode("append").saveAsTable("user2")
        df.write.insertInto("user2")  // 基本等于 mode("append").saveAsTable("user2")
        spark.close()
        
        
    }
}
