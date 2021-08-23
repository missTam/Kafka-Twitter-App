package com.github.mstam.kafka_twitter;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class MyKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    // Define producer configs
    private Properties properties = new Properties();
    // define kafka producer with specified properties
    private KafkaProducer<String, String> producer;

    public MyKafkaProducer() {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void produce(BlockingQueue<String> msgQueue) {

        Iterator<String> tweets = msgQueue.iterator();

        // as long as there are twitter messages in the queue...
        while(tweets.hasNext()) {

            // create record with twitter messages and keep publishing them to kafka in real-time
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", tweets.next());

            // publish data - async
            producer.send(record, new Callback() {

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

    }

    public void close() {
        producer.close();
        producer.flush();
    }
}
