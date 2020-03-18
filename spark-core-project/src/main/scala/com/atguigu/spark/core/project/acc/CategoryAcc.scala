package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

// 为了多讲点东西, 所以用了可变
// in: UserVisitAction out: Map[(品类, "click")-> count]  (品类, "order") -> count  , (品类, "pay") -> count
class CategoryAcc extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {
    self => // 自身类型
    private val map = mutable.Map[(String, String), Long]()
    
    // 判断累加器是否为"零"
    override def isZero: Boolean = map.isEmpty
    
    // 复制累加器
    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
        val acc = new CategoryAcc
        map.synchronized {  // synchronized(锁){}
            acc.map ++= map // 可变集合, 不应该直接赋值, 应该进行数据的复制
        }
        acc
    }
    
    // 重置累加器   这个方法调用完之后, isZero必须返回true
    override def reset(): Unit = map.clear() // 可变集合应该做一个清楚
    
    // 分区内累加
    override def add(v: UserVisitAction): Unit = {
        // 分别计算3个指标
        // 对不同的行为做不同的处理  if语句 或 模式匹配
        v match {
            // 点击行为
            case action if action.click_category_id != -1 =>
                // (cid, "click") -> 100
                val key: (String, String) = (action.click_category_id.toString, "click")
                map += key -> (map.getOrElse(key, 0L) + 1L)
            
            // 下单行为  切出来的是字符串 "null", 不是空的null
            case action if action.order_category_ids != "null" =>
                // 切出来这次下单的多个品类
                val cIds: Array[String] = action.order_category_ids.split(",")
                cIds.foreach(cid => {
                    val key: (String, String) = (cid, "order")
                    map += key -> (map.getOrElse(key, 0L) + 1L)
                })
            
            // 支付行为
            case action if action.pay_category_ids != "null" =>
                val cIds: Array[String] = action.pay_category_ids.split(",")
                cIds.foreach(cid => {
                    val key: (String, String) = (cid, "pay")
                    map += key -> (map.getOrElse(key, 0L) + 1L)
                })
            
            // 其他非正常情况, 做任何处理
            case _ =>
        }
    }
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
        
        // 把other中的map合并到this(self)的map中
        // 合并map
        other match {
            case o: CategoryAcc =>
                // 1. 遍历 other的map,然后把变量的导致和self的map进行相加
                /*o.map.foreach{
                    case ((cid, action), count) =>
                        self.map += (cid, action) -> (self.map.getOrElse((cid, action),0L) + count)
                }*/
                
                // 2. 对other的map进行折叠, 把结果都折叠到self的map中
                // 如果是可变map, 则所有的变化都是在原集合中发生变化, 最后的值可以不用再一次添加
                // 如果是不变map, 则计算的结果, 必须重新赋值给原的map变量
                o.map.foldLeft(self.map) {
                    case (map, (cidAction, count)) =>
                        map += cidAction -> (map.getOrElse(cidAction, 0L) + count)
                        map
                }
            
            case _ =>
                throw new UnsupportedOperationException
        }
        
    }
    
    // 最终的返回值
    override def value: mutable.Map[(String, String), Long] = map
}
