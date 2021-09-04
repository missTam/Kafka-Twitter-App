package com.github.mstam.kafka_twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class HosebirdClient {

    private static Logger logger = LoggerFactory.getLogger(HosebirdClient.class.getName());

    private static String consumerKey;
    private static String consumerSecret;
    private static String token;
    private static String tokenSecret;

    // Setting up a term we want to track, e.g. all tweets containing "kafka"
    private static List<String> terms = Lists.newArrayList("science", "technology", "innovation", "startup");

    private static void readCredentials() {

        try (InputStream input = HosebirdClient.class.getClassLoader().getResourceAsStream("credentials.properties")) {

            Properties properties = new Properties();

            if (input == null) {
                System.out.println("Unable to find credentials.properties");
                return;
            }

            // load properties file from class path
            properties.load(input);

            consumerKey = properties.getProperty("consumerKey");
            consumerSecret = properties.getProperty("consumerSecret");
            token = properties.getProperty("token");
            tokenSecret = properties.getProperty("secret");

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Exception while trying to read from the properties file");
        }

    }

    // hbc will be used to consume twitter API
    public static Client createClient(BlockingQueue<String> msgQueue) {

        readCredentials();

        /** Declare the host to connect to, the endpoint, and authentication */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        /* create client
         * it connects to stream host
         * authenticates with provided OAuth credentials
         * use all defined terms for tracking on the hosebird endpoint
         * use processor which processes the stream and puts individual messages into the BlockingQueue
         */
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
