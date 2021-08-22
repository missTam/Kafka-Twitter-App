package com.github.mstam.kafka_demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {

        String bootsrapServers = "127.0.0.1:9092";

        // Define producer configs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // produce record and publish 10 times
        for(int i = 0; i<10; i++) {

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello kafka " + i);

            // publish data - async
            producer.send(record, new Callback() {

                // executes every time a record is successfully sent or an exception is thrown
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        // success
                        logger.info("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() +  "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset " + recordMetadata.offset() + "\n" +
                                "Timestamp " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while publishing..", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
