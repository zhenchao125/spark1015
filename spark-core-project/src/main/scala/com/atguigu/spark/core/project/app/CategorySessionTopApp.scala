package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/3/18 11:29
 */
object CategorySessionTopApp {
    def statCategorySessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1.过滤出来只包含top10品类的那些点击记录
        // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        // 2. 每个品类top10session的计算
        // 2.1 先map需要字段
        val cidSidAndOne: RDD[((Long, String), Int)] =
        filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        // 2.2 做聚合操作  得到 RDD[((cid, sid), count))]
        val cidSidAndCount: RDD[((Long, String), Int)] = cidSidAndOne.reduceByKey(_ + _)
        // 2.2 map出来想要的数据结构  RDD[(cid, (sid, count))]
        val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndCount.map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        // 2.3 分组 排序取top10
        val cidAndSidCountItRDD: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
        
        // 2.4 对每个value排序取top10
        val result = cidAndSidCountItRDD.mapValues((it: Iterable[(String, Int)]) => {
            // 只能使用scala的排序, scala排序必须把所有数据全部加装内存才能排
            it.toList.sortBy(-_._2).take(10)
        })
    
        result.collect.foreach(println)
    }
    
    
    
}

/*
记算热门session口径: 看每个session的点击记录
1.过滤出来只包含top10品类的那些点击记录
RDD[UserVisitAction]
2. 每个品类top10session的计算
=> RDD[((cid, sid), 1))] reduceByKey
=> RDD[((cid, sid), count))] map
=> RDD[(cid, (sid, count))]  groupByKey
RDD[(cid, Iterator[(sessionId, count), (sessionId, count),...])]  map内部, 对iterator排序, 取前10

-----

问题解决方案:



 */
