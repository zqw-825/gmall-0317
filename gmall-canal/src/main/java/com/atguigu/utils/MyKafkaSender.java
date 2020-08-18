package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zqw
 * @create 2020-08-18 11:51
 */
public class MyKafkaSender {

    //声明kafka生产者
    public static KafkaProducer<String, String> kafkaProducer;

    //创建Kafka生产者方法
    public static KafkaProducer<String, String> createKafkaProducer() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }


    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();

        }
        kafkaProducer.send(new ProducerRecord<>(topic, msg));

    }
}
