package com.berkgokden.db;

import com.berkgokden.csv.IndexableContent;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A helper class to hold Elasticsearch related objects and methods
 * This class designed as Singleton
 * since there can be a single Elasticsearch instance
 * in this project
 */
public class ElasticsearchManager {
    private static final Logger logger = Logger.getLogger(ElasticsearchManager.class.getName());

    private static ElasticsearchManager ourInstance = new ElasticsearchManager();

    public static ElasticsearchManager getInstance() {
        return ourInstance;
    }

    private Client client;
    private BulkProcessor bulkProcessor;

    private List<InetSocketTransportAddress> addresses;

    private ElasticsearchManager() {
        addresses = new LinkedList<>();
    }

    /**
     * User can add addresses before client created
     * @param address String host address ex.: localhost
     * @param port int port value ex.: 9300
     */
    public void addAddress(String address, int port) {
        try {
            addresses.add(new InetSocketTransportAddress(InetAddress.getByName(address), port));
        } catch (UnknownHostException e) {
            logger.error("Unknown host: "+address, e);
        }
    }

    /**
     * A method to ensure client is created and returned correctly
     * Elasticsearch Transportclient should be created after addresses added
     * TODO: add an exception to guarentee addresses added
     * @return created client with given addresses
     */
    private Client getClient() {
        if (client == null) {
                TransportClient transportClient = TransportClient.builder().build();
                for (InetSocketTransportAddress address : addresses) {
                    transportClient.addTransportAddress(address);
                }
                client = transportClient;
        }
        return client;
    }

    /**
     * A method to ensure bulkprocessor is created and returned correctly
     * Elasticseach BulkProcessor regularises the connection to db
     * it handles connections, errors and retries
     *
     * @return BulkProcessor object
     */
    private BulkProcessor getBulkProcessor() {
        if (bulkProcessor == null) {
            bulkProcessor = BulkProcessor.builder(
                getClient(),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        logger.info("execution start: " + executionId);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        logger.info("execution stop: " + executionId);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        logger.error("execution failed: " + executionId, failure);
                    }
                })
                .setBulkActions(100) // default is 1000, for this example 100 is better
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        }
        return bulkProcessor;
    }

    /**
     * A method to get an db object as IndexableContent
     *
     * @param index String index of db object as in Elasticsearch
     * @param type String type of db object as in Elasticsearch
     * @param id String id of db object as in Elasticsearch
     * @return IndexableContent, exact json value can be reached as indexableContent.getContent()
     */
    public IndexableContent get(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id)
                .get();
        IndexableContent indexableContent = new IndexableContent();
        indexableContent.setIndex(response.getIndex());
        indexableContent.setType(response.getType());
        indexableContent.setId(response.getId());
        for (Map.Entry<String, Object> stringObjectEntry : response.getSource().entrySet()) {
            indexableContent.add(stringObjectEntry.getKey(), stringObjectEntry.getValue().toString());
        }
        return indexableContent;
    }

    /**
     * A method to add indexableContent to BulkProcessor to be indexed in Elasticsearch
     * Note that object is not indexed instantly,
     * it waits for more object, a period of time or explicit flush,
     * whichever comes first.
     *
     * @param indexableContent the object ro be indexed
     */
    public void index(IndexableContent indexableContent) {
        getBulkProcessor().add(new IndexRequest(indexableContent.getIndex(),
                indexableContent.getType(),
                indexableContent.getId())
                .source(indexableContent.getContent()));
    }

}
