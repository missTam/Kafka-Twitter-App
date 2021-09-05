package com.github.mstam.elasticsearch;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyKafkaConsumer {

    private static final String INDEX = "twitter_tweet";
    private static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class.getName());

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = ElasticsearchRESTClient.createClient();

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "foo")
                .field("bar", "bar")
                .endObject();

        // create index request
        IndexRequest indexRequest = new IndexRequest(INDEX)
                .source("{\"foo\": \"bar\"}", XContentType.JSON)
                .id(Integer.toString(15));

        // insert json into our index and get id back from elasticsearch
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        client.close();
    }
}
