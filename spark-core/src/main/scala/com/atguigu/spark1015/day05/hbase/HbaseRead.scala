package com.atguigu.spark1015.day05.hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Serialization

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/3/17 10:53
 */
object HbaseRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        // 连接hbase的配置
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
        
        // 从hbase读数据
        val rdd1 = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable], // rowKey封装在这个类型中
            classOf[Result]
        )
        // 读到数据封装下
        val resultRDD = rdd1.map {
            // iw只封装rowKey  result封装一行数据
            case (iw, result) => {
                
                val map = mutable.Map[String, Any]()
                // 把rowKey存入到map中
                map += "rowKew" -> Bytes.toString(iw.get())
                // 再把每一列也存入到map中
                val cells: util.List[Cell] = result.listCells()
                import scala.collection.JavaConversions._
                for (cell <- cells) {
                    // 列名->列值
                    val key = Bytes.toString(CellUtil.cloneQualifier(cell))
                    val value = Bytes.toString(CellUtil.cloneValue(cell))
                    map += key -> value
                }
                // 把map转成json  json4s(json4scala)
                implicit val df = org.json4s.DefaultFormats
                Serialization.write(map)
            }
        }
        resultRDD.collect.foreach(println)
        sc.stop()
        
    }
}
