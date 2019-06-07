package com.kobekun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming完成有状态的词频统计
  *
  * 打成jar包在Linux上运行
  *
  * E:\IdeaProjects\sparkStreaming\out\artifacts\sparkStreaming_jar\sparkStreaming.jar
  *
  *   ./bin/spark-submit \
  *   --class com.kobekun.spark.StatefulWordCount \
  *   --master local[2] \
  *   /home/hadoop/app/submit/sparkStreaming.jar \
  *   100
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //如果使用了stateful的算子，必须设置checkpoint
    // 带状态算子必然要存放之前的值，checkpoint就是设置存放数据的目录，.表示当前的目录
    ssc.checkpoint("file:///home/hadoop/app/submit/kobekun")

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" "))
        .map((_,1))

    val state = result.updateStateByKey[Int](updateFunction _) //隐式转换

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 吧当前的数据去更新已有的或者是老的数据
    * @param currentValues 当前的数据
    * @param preValues 之前的数据
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {

    val current = currentValues.sum   // add the new values with the previous running count to get the new count

    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
