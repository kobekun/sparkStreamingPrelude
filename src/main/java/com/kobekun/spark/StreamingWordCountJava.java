package com.kobekun.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用java开发sparkstreaming应用程序
 */
public class StreamingWordCountJava {

    public static void main(String[] args) throws Exception{

        SparkConf sparkConf = new SparkConf().setAppName("StreamingWordCountJava")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        //创建 receiverInputDStream
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost",9999);

        //将receiverInputDStream转化成JavaPairDStream
        JavaPairDStream<String,Integer> counts = lines.flatMap(line ->
                Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word ->
                        new Tuple2<String,Integer>(word,1))
                .reduceByKey((x,y) ->(x+y));

        //输出
        counts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
