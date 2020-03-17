package com.atguigu.spark1015.day05.jdbc

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 9:55
 */
object DBCRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DBCRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val user = "root"
        val password = "aaaaaa"
        val rdd = new JdbcRDD[String](
            sc,
            () => {
                // 建立到mysql的连接
                // 1. 加载驱动
                Class.forName("com.mysql.jdbc.Driver")
                // 2. 获取连接
                DriverManager.getConnection(url, user, password)
            },
            "select * from user where id >= ? and id <= ?",
            1,
            10,
            2,
            (resultSet: ResultSet) => resultSet.getString(2)
        )
        rdd.collect.foreach(println)
        sc.stop()
        
    }
}

/*
把jdbc(mysql)的数据直接读到rdd中
 */