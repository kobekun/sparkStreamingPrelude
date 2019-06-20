package com.kobekun.spark.kafka;

/**
 * kafka java api测试
 */
public class KafkaClient {

    public static void main(String[] args) {

        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
