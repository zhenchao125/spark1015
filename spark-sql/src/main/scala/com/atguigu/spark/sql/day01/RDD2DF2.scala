package com.atguigu.spark.sql.day01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object RDD2DF2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext
        
        val rdd= sc.parallelize(("lisi", 10) :: ("zs", 20) :: Nil).map{
            case (name, age) => Row(name, age)
        }
        val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
        // 使用提供了一些api
        val df = spark.createDataFrame(rdd, schema)
        df.show
        spark.stop()
    }
}

