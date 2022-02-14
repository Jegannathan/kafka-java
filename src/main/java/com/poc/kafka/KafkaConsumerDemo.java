package com.poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);
        String bootstrapServer = "192.168.1.6:9092";
        String topic= "first-topic";
        String groupId = "second-application";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, String> consumersRecord =  kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> consumerRecord : consumersRecord) {
                logger.info("Topic " + consumerRecord.topic() + "/n"
                + "message " + consumerRecord.value()  + "/n" + "key " + consumerRecord.key()  + "/n"
                );
            }
        }
    }
}
