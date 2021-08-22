package com.github.mstam.kafka_demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// assign & seek is a less used API
// use-cases: replay data or fetch specific message
public class ConsumerAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class.getName());

        Properties properties = new Properties();

        String bootsrapServers = "127.0.0.1:9092";
        // there is no group id
        String topic = "first_topic";

        // define consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        // Consumer takes bytes and needs to deserialize it (convert to a given type)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /*
        + earliest = read from the beginning of the topic
        * latest = only from new messages onwards
        * none = throws error if there is no offset saved
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // there is no subscribing to a topic, instead..
        // assign - this consumer should read this topic partition from the designated offset
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(List.of(partitionToReadFrom));

        // seek specific offset
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int numberOfMessagesRead = 0;
        
        // records are polled from Kafka in batches
        while(keepReading) {
            //
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                numberOfMessagesRead++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if(numberOfMessagesRead >= numberOfMessagesToRead) {
                    keepReading = false;
                    break; // to exit the foor loop
                }
            }
        }
    }
}
