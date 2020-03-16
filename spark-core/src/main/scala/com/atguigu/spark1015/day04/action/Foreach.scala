package com.atguigu.spark1015.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/16 9:54
 */
object Foreach {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Foreach").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        /*rdd1.foreach(x => {
            // 先建立到mysql
            
            // 写数据到mysql
            
            // 关闭连接
        })*/
        
        rdd1.foreachPartition(it => {
            println("foreachPartition ...")
            // it表示每个分区内的所有数据
            // 每个建一个连接
            
            // 遍历迭代器使用这个分区的连接写入数据
            
            
            // 分区写完要关闭连接
        })
        
        sc.stop()
        
    }
}

/*
foreach:
    1. 遍历每个元素
    2. 这个遍历的操作是在executor上完成
    3. 这个和scala的foreach不一样, 原来是先把数据都拉倒驱动, 然后使用
        scala的foreach
    4. 和map的对象: map是转换算子, map也是遍历,会一进一出. foreach是行动算子, 一进不出
    5. 一般用于与外部存储进行通讯
    
数据写入到mysql:
    思路1:
         1. 先把数据拉取到驱动, 然后由驱动统一的向mysql写入
         2. 建立到mysql的连接(jdbc)
         3. 遍历数组,依次写入
         
         好处:
            只需要一个mysql连接
         坏处:
            所有数据到内存, 对驱动端的内存压力比较大
            
    思路2:
        在executor数据计算完毕, 直接写入到mysql
        1. 使用行动算子foreach, 分别写入到mysql
        
        好处:
            遍历一个元素写一次,对内存没有任何压力
        坏处:
            每写一个元素都需要建立一个到mysql的连接.
            到mysql的连接数会过多, 对mysql的压力比较大
            不能在驱动建立连接, 然后到executor上使用, 连接不能序列化
            
    思路3:
        一个分区建立一个到mysql的连接, 这个分区内所有的数据, 可以共用这一个连接
        连接数和分区数一致
        
    
 */
