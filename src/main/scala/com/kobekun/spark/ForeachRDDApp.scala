package com.kobekun.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming完成词频统计,将结果写入到mysql
  *
  *   E:\IdeaProjects\sparkStreaming\src\main\scala
  *   \com\kobekun\spark\ForeachRDDApp.scala
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {
    //创建spark配置
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")
    //创建streaming上下文环境
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //获取socket数据
    val lines = ssc.socketTextStream("localhost",6789)
    //对数据进行算子运算
    val result = lines.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)


    //TODO 将结果写入到数据库中

//    ERROR scheduler.JobScheduler: Error running job streaming job 1559879440000 ms.0
//    org.apache.spark.SparkException: Task not serializable

//    result.foreachRDD { rdd =>
//
//      val connection = createConnection() // executed at the driver 连接不能被序列化
//
//      rdd.foreach { record =>
//
//        //注意后面单双引号的用法
//        val sql = "insert into wordcount(word,wordcount) values('"+record._1+"','"+record._2+"');"
//
//        connection.createStatement().execute(sql)
//      }
//    }
        //分区的作用是 分区的rdd公用一个connection
        result.foreachRDD { rdd =>
          //将rdd分成若干个partition
          rdd.foreachPartition { partitionOfRecords =>
            //每个partition创建一个connection
            val connection = createConnection()
            //对partition里面的每条记录进行遍历
            partitionOfRecords.foreach(record => {
              //注意后面单双引号的用法
              val sql = "insert into wordcount(word,wordcount) values('"+record._1+"','"+record._2+"');"
              //一个partition使用一个connection进行数据库操作
              connection.createStatement().execute(sql)
            })
            //对应前面的创建，要关闭连接
            connection.close()
          }

        }

    ssc.start()

    ssc.awaitTermination()
  }

  /**
    * 获取数据库连接
    */
  def createConnection(): Connection ={

    Class.forName("com.mysql.jdbc.Driver")

    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark"
      ,"root","881105")

    conn
  }
}
