package com.berkgokden.task;

/**
 * Created by following this tutorial:
 * https://dzone.com/articles/mocking-rabbitmq-for-integration-tests
 */
import java.io.*;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import com.google.common.io.Files;
public class EmbeddedAMQPService {
    public static final int BROKER_PORT = 5672;
    private final Broker broker = new Broker();

    public EmbeddedAMQPService() throws Exception {
        final String configFileName = "qpid-config.json";
        final String passwordFileName = "passwd.properties";
        // prepare options
        final BrokerOptions brokerOptions = new BrokerOptions();
        brokerOptions.setConfigProperty("qpid.amqp_port", String.valueOf(BROKER_PORT));
        brokerOptions.setConfigProperty("qpid.pass_file", findResourcePath(passwordFileName));
        brokerOptions.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
        brokerOptions.setInitialConfigurationLocation(findResourcePath(configFileName));
        // start broker
        broker.startup(brokerOptions);
    }

    public void shutdown() {
        broker.shutdown();
    }

    private String findResourcePath(final String file) throws IOException {
        return getClass().getClassLoader().getResource(file).getPath();
    }
}