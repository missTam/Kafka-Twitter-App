package com.github.mstam.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroups {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerGroups.class.getName());

        Properties properties = new Properties();

        String bootsrapServers = "127.0.0.1:9092";
        /*
        * every time a consumer comes up or down, they re-balance
        * if we run consumer multiple times, it will re-balance the topic with 3 partitions:
        * e.g. 2 consumer instances - 1 will read from one partition, 2nd from the remaining two
        * e.g. 3 instances: each will only read messages from one partition
        * See: ConsumerCoordinator logs
        */
        String groupId = "my-app-group";
        String topic = "first_topic";

        // define consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        // Consumer takes bytes and needs to deserialize it (convert to a given type)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        /*
        + earliest = read from the beginning of the topic
        * latest = only from new messages onwards
        * none = throws error if there is no offset saved
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
        // singleton only subscribes to one topic; Arrays.asList() for more, e.g.
        consumer.subscribe(Collections.singleton(topic));
        
        // records are polled from Kafka in batches
        while(true) {
            //
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            });
        }
    }
}
