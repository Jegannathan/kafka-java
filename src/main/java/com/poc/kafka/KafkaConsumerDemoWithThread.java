package com.poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerDemoWithThread {
    private static final CountDownLatch latch = new CountDownLatch(1);
    Logger logger = LoggerFactory.getLogger(KafkaConsumerDemoWithThread.class);
    public static void main(String[] args) {
        new KafkaConsumerDemoWithThread().run();
    }

    private KafkaConsumerDemoWithThread() {

    }

    private void run() {
        String bootstrapServer = "192.168.1.6:9092";
        String topic= "first-topic";
        String groupId = "second-application";
        ConsumerRunnable consumerRunnable =  new ConsumerRunnable(
                bootstrapServer, topic, groupId, latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

       Runtime.getRuntime().addShutdownHook(new Thread(()-> {
           logger.info("Caught Shutdown Hook");
           consumerRunnable.shutDown();
           try {
               latch.await();
           } catch (InterruptedException e) {
               logger.error("Thread is interrupted",e);
           } finally {
               logger.info("Application is terminated");
           }
       }));



        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Thread is interrupted",e);
        } finally {
            logger.info("Application is terminated");
        }

    }
   public class ConsumerRunnable implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> kafkaConsumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServer, String topic, String groupId, CountDownLatch latch) {
            Properties properties = new Properties();
            this.latch = latch;
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumersRecord =  kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String,String> consumerRecord : consumersRecord) {
                        logger.info("Topic " + consumerRecord.topic() + "/n"
                                + "message " + consumerRecord.value()  + "/n" + "key " + consumerRecord.key()  + "/n"
                        );
                    }
                }
            } catch(WakeupException e) {
                logger.info("Received ShutDown SIGNAL");
            } finally {
                kafkaConsumer.close();
                // Tell our main code we are done with the consumer.
                latch.countDown();
            }

        }
        public void shutDown() {
            // wakeup method is used to interrupt the consumer poll
            // It will Throw WakeUp Exception
            kafkaConsumer.wakeup();
        }
    }
}


