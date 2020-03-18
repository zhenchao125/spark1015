package com.atguigu.spark.core.project.app

import java.util

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
    
    /*
    解决方案1: 每次排序一个cid, 需要排10次
     */
    def statCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1.过滤出来只包含top10品类的那些点击记录
        // 1.1 先top10品类id拿出来, 转成Long id的目的是为了和UserVisitAction clickId兼容
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        // 2. 需要排10次
        val temp = cids.map(f = cid => {
            // 2.1 过滤出来点击id是cid的那些记录
            val cidUserVisitActionRDD: RDD[UserVisitAction] = filteredUserVisitActionRDD.filter(_.click_category_id == cid)
            // 2.2 聚合
            val r = cidUserVisitActionRDD
                .map(action => ((action.click_category_id, action.session_id), 1))
                .reduceByKey(_ + _)
                .map {
                    case ((cid, sid), count) => (cid, (sid, count))
                }
                .sortBy(-_._2._2)
                .take(10)
                .groupBy(_._1)
                .map {
                    case (cid, arr) => (cid, arr.map(_._2).toList)
                }
            r
        })
       val result =  temp.flatMap(map => map)
        result.foreach(println)
    }
    
    /*
    解决方案3:
        找一个可以排序的集合, 然后时刻保持这个集合中只有10最大的元素.
     */
    def statCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
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
            // 不要把Iterable直接转成list再排序.
            var set: mutable.TreeSet[SessionInfo] = mutable.TreeSet[SessionInfo]()
            it.foreach{
                case (sid, count) =>
                    val info = SessionInfo(sid, count)
                    set += info
                    if(set.size > 10) set = set.take(10)
            }
            set.toList
        })
        
        // 起 1 job
        result.collect.foreach(println)
        Thread.sleep(1000000)
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

使用scala的排序, 会导致内存溢出
问题解决方案:
    方案2:
        1. 用spark排序,来解决问题.
        2. spark的排序是整体排序. 不能直接使用spark排序
        3. 10个品类id, 我就使用spark的排序功能排10次.
        
        优点:
            一定能完成, 不会oom
        缺点:
            起 10 job, 排序10次
            
     方案3:
        内存溢出, iterable => 转换list
        
        最终的目的top10
        搞一个集合, 这集合中永远只保存10个元素, 用于最大的10 个元素
        
        先聚合, 聚合后分组, 分组内做了排序(用了自动排序的功能集合TreeSet)
        
        优点:
            一定可以完成, 也不会oom, job也是只有一个job
            
        坏处:
            做了两次shuffle, 效率比较地下
            
      方案4:
        对方案3做优化, 减少一次shuffle
            
      
        



 */
