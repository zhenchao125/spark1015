package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/3/23 10:41
 */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val sourceStream = ssc.receiverStream(new MyReceiver("hadoop102", 9999))
        
        sourceStream
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

// 接收器从socket接受数据
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    
    var socket: Socket = _
    var reader: BufferedReader = _
    
    override def onStart(): Unit = {
        // 连接socket, 去读取数据  socket 编程
        runInThread {
            try {
                socket = new Socket(host, port)
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
                var line = reader.readLine()
                // 当对方发送一个流结束标志的时候, 会受到null
                while (line != null && socket.isConnected) {
                    store(line)
                    line = reader.readLine() // 如果流中没有数据, 这里会一直阻塞
                }
            } catch {
                case e => e.printStackTrace()
            } finally {
                restart("重启接收器")
                // 自动立即调用onStop, 然后再调用onStart
            }
        }
    }
    
    // 在一个子线程中去执行传入的代码
    def runInThread(op: => Unit) = {
        new Thread() {
            override def run(): Unit = op
        }.start()
    }
    
    // 释放资源
    override def onStop(): Unit = {
        if (socket != null) socket.close()
        if (reader != null) reader.close()
    }
}