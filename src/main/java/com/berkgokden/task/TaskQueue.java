package com.berkgokden.task;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

/**
 * A helper class to hold AMQP related objects and methods
 * This class designed as Singleton
 * since there can be a single TaskQuer instance
 * in this project
 *
 * RabbitMQ is used in this project but this queue can connect o any AMQP instance.
 * In tests another AMQP implementation is used.
 *
 * Tasks are queued at channels intead of an Exchange to make this project simpler
 */
public class TaskQueue {
    private static final Logger logger = Logger.getLogger(TaskQueue.class.getName());

    private static final String TASK_QUEUE_NAME = "task_queue";

    private static TaskQueue ourInstance = new TaskQueue();

    public static TaskQueue getInstance() {
        return ourInstance;
    }

    private ConnectionFactory factory ;

    private String host = "localhost";

    private TaskQueue() {
        factory = new ConnectionFactory();
    }

    /**
     * A helper method to publish a string value to task_queue
     * Note that connections are not management is done by ConnectionFactory
     *
     * @param message
     * @throws IOException
     * @throws TimeoutException
     */
    public void publish(String message) throws IOException, TimeoutException {
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        channel.basicPublish( "", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
        logger.debug(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    /**
     * A helper function to register a consumer function to wait on task_quue
     * To enable retries given function should return true when process successful
     * and false when retry is required.
     *
     * @param function Function should get a string and return true if process successful
     * @throws IOException
     * @throws TimeoutException
     */
    public void registerConsumerFunction(Function<String, Boolean> function) throws IOException, TimeoutException {
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                logger.debug(" [x] Received '" + message + "'");
                try {
                    if (function.apply(message)) {
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    } else {
                        channel.basicNack(envelope.getDeliveryTag(), false, true);
                    }
                } catch (IOException ex) {
                    logger.error("Handle Delivery Error:", ex);
                }
            }
        };

        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }

    public String getHost() {
        return host;
    }

    /**
     * A method set AMQP service host address
     * it is possible to set multiple addresses when creating connection
     * In this example only one task server is used
     * @param host address of AMQP service
     */
    public void setHost(String host) {
        this.host = host;
        factory.setHost(host);
    }
}
