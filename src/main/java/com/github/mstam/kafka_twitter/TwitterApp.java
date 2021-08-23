package com.github.mstam.kafka_twitter;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterApp {

    Logger logger = LoggerFactory.getLogger(TwitterApp.class.getName());

    public TwitterApp() {}

    public static void main(String[] args) {
        new TwitterApp().start();
    }

    private void start() {

        // client will put the extracted msgs into a blocking queue
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create client for consuming twitter API
        Client client = HosebirdClient.createClient(msgQueue);
        client.connect();

        // create kafka producer
        MyKafkaProducer producer = new MyKafkaProducer();

        /*
        * Shutdown Hook
        * An initialized un-started thread.
        * When the virtual machine begins its shutdown sequence it starts all registered shutdown hooks
        * in unspecified order and lets them run concurrently. Once all hooks have finished, VM halts.
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("application stopping..");
            logger.info("twitter (hbc) client shutting down..");
            client.stop();
            logger.info("closing kafka producer..");
            producer.close();
        }));

        // test the client
        // msgQueue will now start being filled with messages. Read from these queues and log the msg.
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                producer.publish(msg);
                logger.info(msg);
            }
        }
        logger.info("End of application");
        producer.close();
    }


}
