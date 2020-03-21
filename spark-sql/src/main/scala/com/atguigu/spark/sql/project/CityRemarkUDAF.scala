package com.atguigu.spark.sql.project

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Author atguigu
 * Date 2020/3/21 14:35
 */
class CityRemarkUDAF extends UserDefinedAggregateFunction {
    // 输入的数据类型   "北京", "天津"  String
    override def inputSchema: StructType = ???
    
    // 缓冲类型  每个地区的每个商品 缓冲所有城市的点击量   北京->1000  天津->100   石家庄->2
    override def bufferSchema: StructType = ???
    
    // 最终聚合结果的类型   北京21.2%，天津13.2%，其他65.6%  String
    override def dataType: DataType = ???
    
    // 确定性
    override def deterministic: Boolean = true
    
    // 对缓冲区做初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = ???
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???
    
    // 返回最后的聚合结果
    override def evaluate(buffer: Row): Any = ???
}