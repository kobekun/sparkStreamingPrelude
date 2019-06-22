package com.kobekun.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerForOffset {

    public static void main(String[] args) {

        String topic = "imooc_pk_offset";

        Properties properties = new Properties();

        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        properties.put("partitioner.class","kafka.producer.DefaultPartitioner");

        Producer<String,String> producer = new Producer<>(new ProducerConfig(properties));

        for(int i=0; i<100; i++){

            producer.send(new KeyedMessage<>(topic, i+"","kobekun: "+ UUID.randomUUID()));

        }

        System.out.println("kobekun kafka生产者生产数据完毕。。。");
    }
}
