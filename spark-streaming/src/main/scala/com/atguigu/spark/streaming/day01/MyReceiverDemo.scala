package com.atguigu.spark.streaming.day01

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * Author atguigu
 * Date 2020/3/23 10:41
 */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        
    }
}
// 接收器从socket接受数据
class MyReceiver extends Receiver(StorageLevel.MEMORY_ONLY) {
    override def onStart(): Unit = ???
    
    override def onStop(): Unit = ???
}