package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/3/18 9:29
 */
object CategoryTopApp {
    def calcCategoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {
        
        // 使用累加器完成3个指标的累加:   点击 下单量 支付量
        val acc = new CategoryAcc
        sc.register(acc)
        userVisitActionRDD.foreach(action => acc.add(action))
        // 1. 把一个品类的三个指标封装到一个map中
        val cidActionCountGrouped: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
        // 2. 把结果封装到样例类中
        val categoryCountInfoArray: Array[CategoryCountInfo] = cidActionCountGrouped.map {
            case (cid, map) =>
                CategoryCountInfo(cid,
                    map.getOrElse((cid, "click"), 0L),
                    map.getOrElse((cid, "order"), 0L),
                    map.getOrElse((cid, "pay"), 0L)
                )
        }.toArray
        // 3. 对数据进行排序取top10
        val result = categoryCountInfoArray
            .sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
            .take(10)
        
        result.foreach(println)
        // 4.写到jdbc中 TODO
        
    }
}

/*
利用累计器完成
 */