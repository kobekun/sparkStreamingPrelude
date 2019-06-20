package com.kobekun.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread {

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    //获取消费者connector
    private ConsumerConnector createConnector(){

        Properties properties = new Properties();

        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public void run(){

        ConsumerConnector consumerConnector = createConnector();

        Map<String,Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(topic,1);

        //KafkaStream<byte[],byte[]>对应的数据流 通过消费者connector创建消息流
        Map<String, List<KafkaStream<byte[],byte[]>>> messageStream = consumerConnector
                .createMessageStreams(topicCountMap);

        //获取每次接收到数据
        KafkaStream<byte[],byte[]> stream = messageStream.get(topic).get(0);

        //kafka流迭代器
        ConsumerIterator<byte[],byte[]> iterator = stream.iterator();

        while (iterator.hasNext()){
            String message = new String(iterator.next().message());

            System.out.println("rec: " + message);
        }
    }
}
