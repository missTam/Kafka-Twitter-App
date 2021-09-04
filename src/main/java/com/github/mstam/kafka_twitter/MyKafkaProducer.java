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

        // make producer safe - idempotent
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // idempotency includes by default:
        // all in sync replicas (partition leader included) have to send an acknowledgement back to the producer that they have the data
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // in case of failure, producer will resend the record up to x number of times
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // max no of unacknowledged requests the client will send on a single connection before blocking
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // increase throughput of producer at the cost of some CPU
        // compress txt heavy data
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // introduce small delay to increase chances of msgs being sent together in a batch
        // --> ultimately get more tweets at kafka at a time
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        // increase batch size to 32 KB
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

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
