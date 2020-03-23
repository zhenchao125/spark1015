package com.atguigu.spark.sql.project

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Author atguigu
 * Date 2020/3/21 14:35
 *
 */
class CityRemarkUDAF extends UserDefinedAggregateFunction {
    
    // 输入的数据类型   "北京", "天津"  String
    override def inputSchema: StructType = StructType(Array(StructField("city", StringType)))
    
    // 缓冲类型  每个地区的每个商品 缓冲所有城市的点击量
    //1.  Map(北京->1000  天津->100   石家庄->2)
    //2. 总的点击量
    override def bufferSchema: StructType =
        StructType(Array(StructField("map", MapType(StringType, LongType)), StructField("total", LongType)))
    
    // 最终聚合结果的类型   北京21.2%，天津13.2%，其他65.6%  String
    override def dataType: DataType = StringType
    
    // 确定性
    override def deterministic: Boolean = true
    
    // 对缓冲区做初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String, Long]()
        buffer(1) = 0L
    }
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        input match {
            case Row(cityName: String) =>
                // 1. 总的点击量 + 1
                buffer(1) = buffer.getLong(1) + 1L
                // 2. 给这个城市的点击量 + 1   => 找到缓冲的map, 取出来这个城市原来的点击 + 1, 再赋值过去
                val map = buffer.getMap[String, Long](0)
                buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
            case _ =>
        }
    }
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val map1 = buffer1.getMap[String, Long](0)
        val map2 = buffer2.getMap[String, Long](0)
        
        val total1 = buffer1.getLong(1)
        val total2 = buffer2.getLong(1)
        
        // 1. 总数的聚合
        buffer1(1) = total1 + total2
        // 2. map的聚合
        buffer1(0) = map1.foldLeft(map2) {
            case (map, (cityName, count)) =>
                map + (cityName -> (map.getOrElse(cityName, 0L) + count))
        }
        
    }
    
    // 返回最后的聚合结果
    override def evaluate(buffer: Row): String = {
        // 北京21.2%，天津13.2%，其他65.6%
        val cityAndCount = buffer.getMap[String, Long](0)
        val total = buffer.getLong(1)
        
        val cityCountTop2 = cityAndCount.toList.sortBy(-_._2).take(2)
        
        var cityRemarks = cityCountTop2.map {
            case (cityName, count) => CityRemark(cityName, count.toDouble / total)
        }
        //        CityRemark("其他", 1 - cityRemarks.foldLeft(0D)(_ + _.cityRadio))
        cityRemarks :+= CityRemark("其他", cityRemarks.foldLeft(1D)(_ - _.cityRadio))
        cityRemarks.mkString(",")
    }
}


case class CityRemark(cityName: String, cityRadio: Double){
    val f = new DecimalFormat("0.00%")
    // 北京21.2%
    override def toString: String = s"$cityName:${f.format(cityRadio.abs)}"
}