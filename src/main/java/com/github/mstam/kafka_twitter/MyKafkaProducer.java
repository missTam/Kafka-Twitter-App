package com.github.mstam.kafka_twitter;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "twitter_topic";
    // Define producer configs
    private final Properties properties = new Properties();
    // define kafka producer with specified properties
    private final KafkaProducer<String, String> producer;

    public MyKafkaProducer() {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void publish(String tweet) {

        // create record with the twitter message and publish it to kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, tweet);

        // publish data - async
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    logger.error("Error while publishing tweet.", e);
                }
            }
        });
    }

    public void close() {
        producer.close();
        producer.flush();
    }
}
