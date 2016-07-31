package com.berkgokden;

import com.berkgokden.csv.CSVProcessor;
import com.berkgokden.csv.parsers.WeatherParser;
import com.berkgokden.db.ElasticsearchManager;
import com.berkgokden.ftp.FtpManager;
import com.berkgokden.task.TaskQueue;
import com.berkgokden.ftp.FtpObserver;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Main App
 *
 * Reads input from commandline
 * And prints output to commandline
 *
 * TODO: input parameter validation
 */
public class App 
{
    private static final Logger logger = Logger.getLogger(App.class.getName());
    private static Map<String, String> parameters;

    public static void main( String[] args )
    {
        logger.info( "Ftp 2 database" );
        System.out.println("Ftp 2 database");

        if (args.length <= 0 || args.length%2 == 0) {
            System.err.println("Argument length must be an odd number.");
            System.err.println("USAGE: java -jar executable {TYPE} {PARAMETERS}");
            System.err.println("TYPE: observer,worker");
            System.err.println("Parameters:");
            System.err.println("FTP: ftpserver, ftpusername, ftppassword, ftpdirectory");
            System.err.println("AMQP: amqpserver");
            System.err.println("Elasticearch: elasticsearchhost");
            System.exit(1);
        }

        parameters = new HashMap<>();
        parameters.put("type", args[0]);
        for (int i = 1; i < args.length; i+=2 ) {
            parameters.put(args[i].replace("-","").toLowerCase().trim(), args[i+1]);
        }

        // configure ftp server
        String server = parameters.get("ftpserver");
        String user = parameters.get("ftpusername");
        String password = parameters.get("ftppassword");

        FtpManager ftpManager = FtpManager.getInstance();
        ftpManager.setServer(server);
        ftpManager.setUser(user);
        ftpManager.setPassword(password);

        String amqpServer = parameters.getOrDefault("amqpserver", "localhost");
        TaskQueue taskQueue = TaskQueue.getInstance();
        taskQueue.setHost(amqpServer);

        if ("observer".equals(parameters.get("type"))) {
            runAsObserver();
        } else {
            runAsWorker();
        }

        // These services designed to work until killed.
        runForever();
    }


    public static void runForever() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void runAsObserver() {
        logger.info("Observer");

        Long period = 1000L;
        Long timeStamp = 0L;

        String folder = parameters.getOrDefault("ftpdirectory", "");

        FtpObserver ftpObserver = new FtpObserver(FtpManager.getInstance(), folder, period, timeStamp,
                ftpFile -> {
                    int retry = 10;
                    while (retry-- > 0) {
                        try {
                            TaskQueue.getInstance().publish(ftpFile.getName());
                            // break when no exception
                            retry = -1;
                            break;
                        } catch (IOException e) {
                            logger.error("AMQP io failed:", e);
                        } catch (TimeoutException e) {
                            logger.error("AMQP io timedOut:", e);
                        }
                        if (retry != -1) {
                            logger.error("Could not publish message to AMQP");
                        }
                    }
                });

        ftpObserver.start();
    }

    public static void runAsWorker() {
        logger.info("worker");

        String folder = parameters.getOrDefault("ftpdirectory", "");

        String elasticsearchhosts = parameters.getOrDefault("elasticsearchhost", "localhost");

        ElasticsearchManager elasticsearchManager = ElasticsearchManager.getInstance();

        for (String hostInfo : elasticsearchhosts.split(",")) {
            String[] serverport = hostInfo.split(":");
            String part1 = serverport[0];
            int part2 = 9300;
            if (serverport.length == 2) {
                part2 = Integer.parseInt(serverport[0]);
            }
            elasticsearchManager.addAddress(part1, part2);
        }
        boolean registered = false;

        // TODO: fix re-registering if queue is completely re-started
        while (!registered) {
            try {
                TaskQueue.getInstance().registerConsumerFunction(s -> {
                    try {
                        logger.debug("1 - Received filename for processing : " + s);
                        byte[] compressedData = FtpManager.getInstance().getFile(folder + s);
                        if (compressedData == null) {
                            logger.error("Ftp file download failed for " + s);
                            return false;
                        }
                        logger.debug("2 - Received file for uncompression : " + s);
                        byte[] uncompressedData = FtpManager.uncompress(compressedData);
                        logger.debug("3 - Received data for csv parsing : " + s);
                        CSVProcessor.processCSVdata(uncompressedData,
                                WeatherParser.delimeter,
                                WeatherParser.Headers.class,
                                WeatherParser::parse,
                                ElasticsearchManager.getInstance()::index);
                        logger.debug("4 - Finished processing : " + s);
                    } catch (Exception e) {
                        logger.error("Ftp file processing failed for " + s, e);
                        return false;
                    }
                    return true;
                });
                registered = true;
                break;
            } catch (IOException e) {
                logger.error("AMQP io failed:", e);
            } catch (TimeoutException e) {
                logger.error("AMQP io timedOut:", e);
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error("Thread sleep:", e);
                ;
            }
        }
    }

}
