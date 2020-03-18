package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

// in: UserVisitAction out: Map[(品类, "click")-> count]  (品类, "order") ->    (品类, "pay") ,-> count
class CategoryAcc extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]]{
    // 判断累加器是否为"零"
    override def isZero: Boolean = ???
    // 复制累加器
    override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = ???
    
    // 重置累加器   这个方法调用完之后, isZero必须返回true
    override def reset(): Unit = ???
    
    // 分区内累加
    override def add(v: UserVisitAction): Unit = ???
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = ???
    
    // 最终的返回值
    override def value: Map[(String, String), Long] = ???
}
