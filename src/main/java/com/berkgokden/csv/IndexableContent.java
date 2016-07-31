package com.berkgokden.csv;

import org.apache.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * A class to create and hold data to read/write elasticsearch
 *
 * TODO: refactor this class to rebuild create content only when getContent called
 */
public class IndexableContent {
    private static final Logger logger = Logger.getLogger(IndexableContent.class.getName());
    private String id;
    private XContentBuilder contentBuilder;
    private String index;
    private String type;
    private BytesReference content;

    /**
     * Constructor automatically initialises contentbuilder
     */
    public IndexableContent() {
        try {
            this.contentBuilder = jsonBuilder().startObject();
        } catch (IOException e) {
            logger.error("Content Builder initialization failed: ", e);
        }
    }

    public BytesReference getContent() {
        if (this.content != null) {
            return this.content;
        }
        try {
            this.contentBuilder = this.contentBuilder.endObject();
        } catch (IOException e) {
            logger.error("Content Builder object creation failed.", e);
            return null;
        }
        this.content = this.contentBuilder.bytes();
        return this.content;
    }

    public void add(String key, String value) {
        try {
            this.contentBuilder = this.contentBuilder.field(key, value);
        } catch (IOException e) {
            logger.error("Content Builder field creation failed: ", e);
        }
    }

    public void add(String key, Integer value) {
        try {
            this.contentBuilder = this.contentBuilder.field(key, value);
        } catch (IOException e) {
            logger.error("Content Builder field creation failed: ", e);
        }
    }

    public void add(String key, Long value) {
        try {
            this.contentBuilder = this.contentBuilder.field(key, value);
        } catch (IOException e) {
            logger.error("Content Builder field creation failed: ", e);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
