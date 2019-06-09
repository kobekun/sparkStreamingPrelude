package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkstreaming 整合flume方式：push
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){

      System.err.println("Usage: FlumePushWordCount <hostname> <port>")

      System.exit(1)
    }

//    val hostname = args(0)
//
//    val port = args(1)

    val Array(hostname,port) = args  //和上面写法一样

    //打包之后不需要后面的，需要在命令中指定
    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("FlumePushWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    //flume创建流，并配置sink
    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)
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

//启动flume报错：[WARN - org.apache.flume.sink.AbstractRpcSink.start(AbstractRpcSink.java:294)]
//Unable to create Rpc client using hostname: xxx.xxx.xxx.xxx, port: 41100
//org.apache.flume.FlumeException: NettyAvroRpcClient { host: 121.41.49.51, port: 41100 }:
// RPC connection error

//报上面错的原因是：服务器中flume_push_streaming.conf的simple-agent.sinks.avro-sink.hostname = local's ip
//此处的IP应该配置你本地而不是远程的IP

//服务器上的telnet localhost 44444模拟flume发数据到idea上的作业，
// (telnet在flume启动之后才能启动，停止之后才能停止)
// 所以 sink的主机名应该是本地的IP  win上打开cmd  -> ipconfig