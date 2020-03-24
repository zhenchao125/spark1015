package com.atguigu.spark.streaming.day02.out

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/24 11:24
 */
object OutDemo1 {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "aaaaaa")
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Window1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        ssc.checkpoint("ck3")
        ssc
            .socketTextStream("hadoop102", 9999)
            .flatMap(_.split("\\W+"))
            .map((_, 1))
//            .reduceByKey(_ + _)
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
            //            .saveAsTextFiles("word", "log")
            /*.foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    // 连接到mysql
                    
                    // 写数据
                    
                    
                    // 关闭mysql
                })
            })*/
            .foreachRDD(rdd => {
                // 把rdd转成df
                // 1. 先创建sparkSession
                val spark = SparkSession.builder()
                    .config(rdd.sparkContext.getConf)
                    .getOrCreate()
                import spark.implicits._
                // 2. 转换
                val df = rdd.toDF("word", "count")
                
                // 3. 写
                df.write.mode("overwrite").jdbc("jdbc:mysql://hadoop102:3306/rdd", "word1015", props)
            })
        ssc.start()
        ssc.awaitTermination()
    }
}
