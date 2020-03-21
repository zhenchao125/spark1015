package com.atguigu.spark.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/21 11:26
 */
object HiveRead {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveRead")
            // 添加支持外置hive
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("show databases")
        spark.sql("use gmall")
        spark.sql("select count(*) from ads_uv_count").show
        spark.close()
    }
}
