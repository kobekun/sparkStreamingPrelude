package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkstreaming 整合flume方式：pull
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){

      System.err.println("Usage: FlumePullWordCount <hostname> <port>")

      System.exit(1)
    }

//    val hostname = args(0)
//
//    val port = args(1)

    val Array(hostname,port) = args  //和上面写法一样

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePullWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    //flume创建流，并配置polling stream
    val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
    //flume发送的数据header和body
    flumeStream.map(x => new String(x.event.getBody.array()).trim)
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()

    ssc.start()

    ssc.awaitTermination()
  }
}

// Choose a machine that will run the custom sink in a Flume agent.
// The rest of the Flume pipeline is configured to send data to that agent.
// Machines in the Spark cluster should have access to the chosen machine
// running the custom sink.

//选择将在Flume代理中运行自定义接收器的计算机。 Flume管道的其余部分配置为将数据发送到该代理。
//Spark群集中的计算机应该可以访问运行自定义接收器的所选计算机。

//push的数据流向：flume(telnet) -> sparkstreaming作业(本地或远程，如果是本地必须配置本地的IP
// 远程的话需要配置远程IP。无论是远程还是本地，streaming作业都可以被任意终止)
//pull的数据流向：flume(telnet) -> custom sink defined by flume conf
// -> sparkstreaming(hostname为flume所在机器的IP，streaming从自定义的sink中拿数据(pull),
// 远程上的作业不能被随意终止，除非停止flume agent)

//push启动顺序：先启动sparkstreaming再启动flume
//pull启动顺序：先启动flume再启动sparkstreaming作业

//生产上大多数用pull