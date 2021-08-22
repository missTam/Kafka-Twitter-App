package com.github.mstam.kafka_demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerBasic {
    public static void main(String[] args) {

        String bootsrapServers = "127.0.0.1:9092";

        // Define producer configs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        // what type of value is being sent to kafka and how it should be serialized (converted) into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer with key (extends) String & value as String
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        // @param topic: where is the record being published?
        // @param value: what is being published?
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello kafka");

        // publish data - async
        producer.send(record);

        // wait for data to be produced
        // flush data
        producer.flush();
        // close producer
        producer.close();
    }
}
