package com.github.mstam.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchRESTClient {

    private static final String HOST = "localhost";
    private static final int PORT = 9200;

    /** create Java REST client to connect to Elasticsearch (data store, search & analytics platform)
    /* low-level cluster: communicate with an Elasticsearch cluster through http
    /* high-level client: exposes API methods and takes care of requests marshalling and responses un-marshalling */
    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(RestClient.builder(
                new HttpHost(HOST, PORT, "http")));
    }
}
