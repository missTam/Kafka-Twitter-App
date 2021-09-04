package com.github.mstam.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootsrapServers = "127.0.0.1:9092";

        // Define producer configs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // produce record and publish 10 times
        for(int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello kafka " + i;
            // providing keys guarantees that the same key always goes to the same partition
            String key = "id_" + i;

            logger.info("Key " + key);

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

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
            }).get(); // blocks .send() making it synchronous, for checking keys - partitions mapping // don't do in production
        }

        producer.flush();
        producer.close();
    }
}
