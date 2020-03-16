package com.atguigu.spark1015.day03.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/14 15:45
 */
object Practice {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        // RDD[点击记录]  map
        val lineRDD = sc.textFile("c:/agent.log")
        //  RDD[(省份, 广告), 1]    reduceByKey
        val provinceAdsOne = lineRDD.map(line => {
            val split: Array[String] = line.split(" ")
            ((split(1), split(4)), 1)
        })
        // RDD[(省份, 广告), 10]    map
        val provinceAdsCount = provinceAdsOne.reduceByKey(_ + _)
        // RDD[(省份, (广告, 10))]  groupByKey
        val provinceAndAdsCount = provinceAdsCount.map {
            case ((pro, ads), count) => (pro, (ads, count))
        }
        // RDD[(省份, Iterable((广告1, 10), (广告2, 7), (广告3. 5), ...))] 做map 只对内部的list排序, 取前3
        val proAndAdsCountGrouped: RDD[(String, Iterable[(String, Int)])] = provinceAndAdsCount.groupByKey()
        // 不用能使用spark提供的排序!  用scala的排序
        val result = proAndAdsCountGrouped.map {
            case (pro, adsCountIt) =>
                // 转换成容器式集合才能排序
                (pro, adsCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
        }
        // 最后在按照省份排的漂亮一点
//        val r = result.sortByKey()
        // 考虑把key变成Int之后再排序
        val r = result.sortBy(_._1.toInt)
        r.collect.foreach(println)
        sc.stop()
        
    }
}

/*
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12
2.	需求: 统计出每一个省份广告被点击次数的 TOP3

倒推法:
=> RDD[点击记录]  map
=> RDD[(省份, 广告), 1]    reduceByKey
=> RDD[(省份, 广告), 10]    map
=> RDD[(省份, (广告, 10))]  groupByKey
=> RDD[(省份, Iterable((广告1, 10), (广告2, 7), (广告3. 5), ...))] 做map 只对内部的list排序, 取前3
RDD[(省份, Iterable((广告1, 10), (广告2, 7), (广告3. 5)))]






 */
