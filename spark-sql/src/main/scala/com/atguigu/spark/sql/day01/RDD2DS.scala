package com.atguigu.spark.sql.day01

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Author atguigu
 * Date 2020/3/20 14:17
 */
object RDD2DS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RDD2DS")
            .getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(Array(User("lisi", 10), User("zs", 20), User("ww", 15)))
        val ds: Dataset[User] = rdd.toDS()
        ds.show()
        spark.close()
        
        
    }
}
