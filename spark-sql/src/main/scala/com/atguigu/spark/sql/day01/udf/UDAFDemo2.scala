package com.atguigu.spark.sql.day01.udf

import com.atguigu.spark.sql.day01
import com.atguigu.spark.sql.day01.User
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import sun.nio.cs.ext.DoubleByteEncoder

import scala.text.DocGroup


/**
 * Author atguigu
 * Date 2020/3/20 16:21
 */
case class Dog(name:String, age: Int)
case class AgeAvg(sum: Int, count: Int){
    def avg = sum.toDouble / count
}
object UDAFDemo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo2")
            .getOrCreate()
        import spark.implicits._
        val ds = List(Dog("大黄", 6), Dog("小黄", 2), Dog("中黄", 4)).toDS()
        // 强类型的使用方式
        val avg = new MyAvg2().toColumn.name("avg")
        val result: Dataset[Double] = ds.select(avg)
        result.show()
        spark.close()
        
    }
}

class MyAvg2 extends Aggregator[Dog, AgeAvg, Double] {
    // 对缓冲区进行初始化
    override def zero: AgeAvg = AgeAvg(0, 0)
    // 聚合(分区内聚合)
    override def reduce(b: AgeAvg, a: Dog): AgeAvg = a match {
            // 如果是dog对象, 则把年龄相加, 个数加1
        case Dog(name, age) => AgeAvg(b.sum + age, b.count + 1)
            // 如果是null, 则原封不动返回
        case _ => b
    }
    
    // 分区间的聚合
    override def merge(b1: AgeAvg, b2: AgeAvg): AgeAvg = {
        AgeAvg(b1.sum + b2.sum, b1.count + b2.count)
    }
    
    // 返回最终的值
    override def finish(reduction: AgeAvg): Double = reduction.avg
    
    // 对缓冲区进行编码
    override def bufferEncoder: Encoder[AgeAvg] = Encoders.product // 如果是样例, 就直接返回这个编码器就行了
    
    // 对返回值进行编码
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}