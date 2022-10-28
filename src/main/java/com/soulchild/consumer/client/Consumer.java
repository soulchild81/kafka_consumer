package com.soulchild.consumer.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class Consumer {
    private final static String TOPIC = "event1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();

        // kafka server host 및 port 설정
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "my-group-id-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer

        // consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // topic 설정
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                // 계속 loop를 돌면서 producer의 message를 띄운다.
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records)
                    System.out.println(record.value());
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
        }
    }
}
