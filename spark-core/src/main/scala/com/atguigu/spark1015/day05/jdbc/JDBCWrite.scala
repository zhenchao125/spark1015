package com.atguigu.spark1015.day05.jdbc

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/3/17 9:58
 */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DBCRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val user = "root"
        val password = "aaaaaa"
        // 先有数据, 然后去写
        //        val rdd = sc.parallelize((20, "zs") :: (30, "lisi") :: (25, "ww") :: Nil)
        val rdd = sc.parallelize(1 to 10000).map(x => (x, "zhiling" + x))
        
        val sql = "insert into user1 values(?, ?) "
        // 连接数过多, 每个元素都需要建立一个到mysql的连接. 实际不能用
        /*rdd.foreach {
            case (age, name) => {
                Class.forName("com.mysql.jdbc.Driver")
                val conn = DriverManager.getConnection(url, user, password)
                val ps = conn.prepareStatement(sql)
                ps.setInt(1, age)
                ps.setString(2, name)
                ps.execute()
                ps.close()
                conn.close()
            }
        }*/
        
        // 应该每个分区建立一个到mysql的连接
        /*rdd.foreachPartition(it => {
            Class.forName("com.mysql.jdbc.Driver")
            val conn = DriverManager.getConnection(url, user, password)
            // 一个元素写一次, 效率不够高
            it.foreach{
                case (age, name) =>
                    val ps = conn.prepareStatement(sql)
                    ps.setInt(1, age)
                    ps.setString(2, name)
                    ps.execute()
                    ps.close()
            }
            
            conn.close()
        })*/
        
        
        rdd.foreachPartition(it => {
            Class.forName("com.mysql.jdbc.Driver")
            val conn = DriverManager.getConnection(url, user, password)
            // 一次写一个批次
            val ps = conn.prepareStatement(sql)
            var count = 0
            it.foreach {
                case (age, name) =>
                    ps.setInt(1, age)
                    ps.setString(2, name)
                    ps.addBatch()
                    count += 1
                    if (count % 100 == 0) {
                        ps.executeBatch()
                        Thread.sleep(3000)
                    }
            }
            // 必须要有
            ps.executeBatch()
            conn.close()
        })
        
        sc.stop()
    }
}
