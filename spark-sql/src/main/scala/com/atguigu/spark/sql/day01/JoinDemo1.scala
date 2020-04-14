package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/4/14 16:15
 */
object JoinDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JoinDemo1")
            .getOrCreate()
        import spark.implicits._
        val df1 = List((1, "lisi"),(1, "liwu"), (2, "zs"), (3, "ww")).toDF("id", "name")
        val df2 = List((1, "math"),(1, "tv"), (2, "english"), (4, "chinese")).toDF("id", "item")
        
        // sql
        println("inner...")
        df1.join(df2, "id").show()
    
        /*println("left...")
        df1.join(df2, Seq("id"), "left").show()
        
        println("full...")
        df1.join(df2, Seq("id"), "full").show()*/
        
        println("semi...")  // 办连接
        df1.join(df2, Seq("id"), "leftsemi").show()
        
        println("anti...")  // 办连接
        df1.join(df2, Seq("id"), "leftanti").show()  // not in  左边有, 右边没有
        
        println("cross...")  // 办连接
//        df1.join(df2, Seq("id"), "cross").show()  // not in  左边有, 右边没有
        df1.crossJoin(df2).show
        
        
        spark.close()
        
        
    }
}
