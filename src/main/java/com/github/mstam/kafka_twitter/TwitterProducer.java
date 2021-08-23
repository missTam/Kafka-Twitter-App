package com.github.mstam.kafka_twitter;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    
    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().start();
    }

    private void start() {

        // client will put the extracted msgs into a blocking queue
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create client for consuming twitter API
        Client client = HosebirdClient.createClient(msgQueue);
        client.connect();

        // create kafka producer
        MyKafkaProducer producer = new MyKafkaProducer();

        // test the client
        // msgQueue will now start being filled with messages. Read from these queues and log the msg.
        while (!client.isDone()) {
            producer.produce(msgQueue);
            /*String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
            }*/
        }
        logger.info("End of application");
        producer.close();
    }


}
