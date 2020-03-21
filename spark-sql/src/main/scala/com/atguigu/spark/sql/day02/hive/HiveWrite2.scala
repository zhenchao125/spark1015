package com.atguigu.spark.sql.day02.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author atguigu
 * Date 2020/3/21 13:34
 */
object HiveWrite2 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveWrite2")
            .enableHiveSupport()
            .config("spark.sql.shuffle.partitions", 10)
            .getOrCreate()
        val df: DataFrame = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("a")
        
        spark.sql("use spark1016")
        val df1 = spark.sql("select * from a")
        val df2 = spark.sql("select sum(age) sum_age from a group by name")
        println(df1.rdd.getNumPartitions)
        println(df2.rdd.getNumPartitions)
//        df1.write.saveAsTable("a1")
        df2.coalesce(1).write.mode("overwrite").saveAsTable("a2")
        spark.close()
        
        
    }
}
