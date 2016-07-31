package com.berkgokden.db;

import com.berkgokden.csv.IndexableContent;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by developer on 7/26/16.
 */
public class ElasticsearchManagerTest {

    private ElasticsearchServer elasticsearchServer;

    @Before
    public void setup() throws Exception {
        elasticsearchServer = new ElasticsearchServer();
        elasticsearchServer.start();
    }

    @After
    public void teardown() throws Exception {
        elasticsearchServer.shutdown();
    }

    @Test
    public void shouldPassWhenWrittenObjectReadCorrectly() throws Exception {
        // ElasticsearchManager
        IndexableContent indexableContent = new IndexableContent();
        indexableContent.setId("test-id");
        indexableContent.setIndex("test-index");
        indexableContent.setType("test-type");
        for (int i=0;i<10;i++) {
            indexableContent.add("test-key"+i, String.valueOf(i));
        }
        ElasticsearchManager elasticsearchManager = ElasticsearchManager.getInstance();
        elasticsearchManager.addAddress("localhost", 9300);
        elasticsearchManager.index(indexableContent);
        Thread.sleep(10000);
        IndexableContent indexableContent1 = elasticsearchManager.get("test-index", "test-type", "test-id");

        assertEquals(indexableContent.getIndex(), indexableContent1.getIndex());
        assertEquals(indexableContent.getType(), indexableContent1.getType());
        assertEquals(indexableContent.getId(), indexableContent1.getId());
        // order of entities in json strutures are different so an object mapper needed.
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> m1 = (Map<String, Object>)
                (om.readValue(indexableContent.getContent().toUtf8(), Map.class));
        Map<String, Object> m2 = (Map<String, Object>)
                (om.readValue(indexableContent1.getContent().toUtf8(), Map.class));
        assertEquals(m1, m2);
    }
}