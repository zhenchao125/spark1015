package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
        
        
        
    }
}
/*
利用累计器完成
 */