package com.atguigu.spark1015.day05.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 10:53
 */
object HbaseWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        
        val job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        val initialRDD = sc.parallelize(
            List(("20000", "apple", "11"),
                ("20001", "banana", "12"),
                ("20002", "pear", "13")))
        
        // 先把rdd数据封装成 TableReduce需要的那种格式
       val hbaseRDD =  initialRDD.map{
            case (rk, name, age) =>
                val rowkey = new ImmutableBytesWritable()
                rowkey.set(Bytes.toBytes(rk))
                val put = new Put(Bytes.toBytes(rk))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age))
                (rowkey, put)
        }
        hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        sc.stop()
    }
}
