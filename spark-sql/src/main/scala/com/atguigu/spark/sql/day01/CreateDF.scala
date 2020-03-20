package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/3/20 11:18
 */
object CreateDF {
    def main(args: Array[String]): Unit = {
        // 1. 先创建及SparkSession
        val spark: SparkSession = SparkSession
            .builder().appName("CreateDF").master("local[2]")
            .getOrCreate()
        // 2. 通过SparkSession创建DF
        val df = spark.read.json("c:/users.json")
        // 3. 对DF做操作(sql)
        // 3.1 创建临时表
        df.createOrReplaceTempView("user")
        // 3.2 查询临时表
        spark.sql(
            """
              |select
              | name,
              | age
              |from user
              |""".stripMargin).show
        // 4. 关闭SparkSession
        spark.stop()
        
    }
}
