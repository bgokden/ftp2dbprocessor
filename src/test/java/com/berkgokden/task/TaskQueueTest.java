package com.berkgokden.task;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by developer on 7/26/16.
 */
public class TaskQueueTest {
    EmbeddedAMQPService service;

    @Before
    public void setup() throws Exception {
        service = new EmbeddedAMQPService();
    }

    @After
    public void teardown() throws Exception {
        service.shutdown();
    }

    @Test
    public void shouldPassWhenAllSentMessagesAreReceived() throws Exception {
        TaskQueue taskQueue = TaskQueue.getInstance();
        taskQueue.setHost("localhost");
        int numberOfMessages = 10;

        Set<String> set = new HashSet<>(numberOfMessages);
        taskQueue.registerConsumerFunction(s -> {
            set.add(s);
            return true;
        });

        for (int i = 0; i < numberOfMessages; i++) {
            taskQueue.publish(String.valueOf(i));
        }

        Thread.sleep(1000);
        assertEquals(numberOfMessages, set.size());
        for (int i = 0; i < numberOfMessages; i++) {
            assert(set.contains(String.valueOf(i)));
        }
    }
}