package com.poc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        String bootstrapServer = "192.168.1.6:9092";
        String topic= "first-topic";
        Logger logger = LoggerFactory.getLogger(KafkaProducerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i =0; i<=10; i++) {
            String key = "key"+i;
            String value = "Hello World"+ i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>(topic,key, value);
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic " + recordMetadata.topic() + "\n"
                        +"Offset " + recordMetadata.offset()
                        );
                    } else {
                        logger.error("Error in exception");
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
