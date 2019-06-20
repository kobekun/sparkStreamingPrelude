package com.kobekun.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * java语言使用spark进行词频统计
 */
public class WordCountJava {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("WordCountJava")
                .master("local[2]")
                .getOrCreate();

        //业务
        JavaRDD<String> lines = spark.read()
                .textFile("C:\\Users\\mouse\\Desktop\\hello.txt")
                .javaRDD();

        JavaRDD<String> words = lines.flatMap(line ->
                Arrays.asList(line.split("\t")).iterator());

        JavaPairRDD<String,Integer> counts = words.mapToPair(word ->
                new Tuple2<String,Integer>(word,1))
                .reduceByKey((x,y) -> (x+y));

        List<Tuple2<String,Integer>> output = counts.collect();

        for(Tuple2<String,Integer> tuple: output){

            System.out.println(tuple._1() + ":" + tuple._2());
        }

        spark.stop();
    }
}
