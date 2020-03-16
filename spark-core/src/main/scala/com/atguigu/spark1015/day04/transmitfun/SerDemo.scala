package com.atguigu.spark1015.day04.transmitfun

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SerDemo")
            // 更换序列化器
            .set("spark.serializer", classOf[Kryo].getName)  // 可以省略
            // 注册那些需要使用Kryo序列化的类
            .registerKryoClasses(Array(classOf[Searcher]))
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        //
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        //需求: 在 RDD 中查找出来包含 query 子字符串的元素
        
        // 是在driver创建的对象
        val searcher = new Searcher("hello")  // 用来查找包含hello子字符串的那些字符串组成的rdd
        // 也是在driver
        val result: RDD[String] = searcher.getMatchedRDD1(rdd)
        result.collect.foreach(println)
    }
}


// query 为需要查找的子字符串
case class Searcher(val query: String) {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s : String) ={
        s.contains(query)
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) ={
        // .filter是在driver调用
        // isMatch方法在executor上执行
        rdd.filter(isMatch)
    }
    def getMatchedRDD2(rdd: RDD[String]) ={
        // query 是对象的属性, 所以类也需要序列化
        rdd.filter(x => x.contains(query))
    }
    
    def getMatchedRDD3(rdd: RDD[String]) ={
        val q = query
        // query 是对象的属性, 所以类也需要序列化
        rdd.filter(x => x.contains(q))
    }
}
/*
序列化:
    1. java自带的序列化
        只需要实现java的一个接口: Serializable
        好处:
            1. 及其简单, 不要做任何额外的工作
            2. java自带, 用起来方便
        坏处
            太重
            1. 序列化速度慢
            2. 序列化之后的size比较大
            
        spark默认是使用的这种序列化
            
    2. hadoop没有使用java的序列化
        hadoop自定义序列化机制: ...Writeable
        
    3. 支持另外一种序列化
        Kyro
        不是spark自定写, 而是一个第3方序列化
        
        
        
方法的序列化:
    把方法所在的类实现接口: Serializable, 这个类创建的对象就可以序列化
属性的序列化:
    1. 和方法的序列化一样
    2. 可以把属性的值存入到一个局部变量, 然后传递局部变量
    
有些值无法序列化:
  1. 与外部存储系统的连接不能序列化
 */