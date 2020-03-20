package com.atguigu.spark.sql.day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

import scala.collection.immutable.Nil

/**
 * Author atguigu
 * Date 2020/3/20 15:20
 */
object UDAFDemo {
    def main(args: Array[String]): Unit = {
        // 在sql中, 聚合函数如何使用
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo")
            .getOrCreate()
        import spark.implicits._
        val df = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("user")
        // 注册聚合函数
        spark.udf.register("mySum",  new MySum)
        spark.sql("select mySum(age) from user").show
        spark.close()
    }
}

class MySum extends UserDefinedAggregateFunction {
    // 输入的数据类型  10.1 12.2 100
    override def inputSchema: StructType = StructType(StructField("ele", DoubleType)::Nil)
    
    // 缓冲区的类型
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType)::Nil)
    
    // 最终聚合解结果的类型
    override def dataType: DataType = DoubleType
    
    // 相同的输入是否返回相同的输出
    override def deterministic: Boolean = true
    
    // 对缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 在缓冲集合中初始化和
        buffer(0) = 0D   // 等价于 buffer.update(0, 0D)
    }
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // input是指的使用聚合函数的时候, 缓过来的参数封装到了Row
        if (!input.isNullAt(0)) {  // 考虑到传字段可能是null
            val v = input.getAs[Double](0)  // getDouble(0)
            buffer(0) = buffer.getDouble(0) + v
        }
    }
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 把buffer1和buffer2 的缓冲弄聚合到一起, 然后再把值写回到buffer1
        buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    }
    
    // 返回最终的输出值
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}