package com.berkgokden.ftp;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPListParseEngine;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A class to track file changed in a ftp server folder.
 * FtpObserver checks given folder for newer files.
 * FtpObserver extends Thread so it can run in the background
 *
 * Example usage:
 * new FtpObserver(ftpManager, folder, period, timeStamp,
 *  ftpFile -> {System.out.println(ftpFile.getName());}
 *  }}).start();
 */
public class FtpObserver extends Thread {
    private static final Logger logger = Logger.getLogger(FtpObserver.class.getName());
    private static final long FIVE_MINUTES_IN_MILLIS = 5*60*1000;

    private Long period;
    private String folder;
    private Map<Long, Set<String>> fileMap;
    private Long timeStamp;
    private boolean keepAlive;
    private Consumer<FTPFile> consumer;
    private FtpManager ftpManager;

    private final static int PAGE_SIZE = 10;

    /**
     * FtpObserver object construnctor
     *
     * @param ftpManager ftpMagager instance required
     * @param folder folder path ex.: /myfolder use "" for root folder.
     * @param period time in miliseconds to wait between checks
     * @param timeStamp timeStamp to start from. use 0L if not known.
     * @param consumer consumer function to process newer files.
     */
    public FtpObserver(FtpManager ftpManager,String folder, Long period, Long timeStamp, Consumer<FTPFile> consumer) {
        this.ftpManager = ftpManager;
        this.period = period;
        this.folder = folder;
        this.timeStamp = timeStamp;
        this.consumer = consumer;
        this.fileMap = new HashMap<>();
        this.keepAlive = true;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    /**
     * Thread.run method allows FtpObserver to run in the background
     * This metod checks periodically ftp server for newer files.
     * Files are checked againt the local file list and timestamp
     * Timestamp of the newer files should be lager than local timestamp
     * and not exist in the local file list.
     * Older files with older Timestamps are cleared periodically to keep
     * this object structure from getting too large.
     */
    @Override
    public void run() {

        while (isKeepAlive()) {
            try {
                FTPListParseEngine engine = ftpManager.getFtp().initiateListParsing(folder);

                long newTimeStamp = timeStamp;
                while (engine.hasNext()) {
                    FTPFile[] files = engine.getNext(PAGE_SIZE);
                    for (FTPFile file : files) {
                        long fileTimeStamp = file.getTimestamp().getTimeInMillis();
                        // Check for files that are added after the recorder time stamp
                        // 5 minutes buffer added to timestamp
                        if (file.isFile()
                                && fileTimeStamp > timeStamp - FIVE_MINUTES_IN_MILLIS) {
                            fileMap.putIfAbsent(fileTimeStamp,
                                    new HashSet<>());
                            boolean added = fileMap.get(fileTimeStamp)
                                    .add(file.getName());

                            if (added) {
                                newTimeStamp = fileTimeStamp;
                                consumer.accept(file);
                            }
                        }
                    }
                }
                if (timeStamp != newTimeStamp) {
                    logger.debug("new timestamp: " + timeStamp);
                }
                timeStamp = newTimeStamp;
                // Delete older files otherwise fileMap size is not bounded.
                fileMap = fileMap.entrySet().stream()
                        .filter(map -> map.getKey() >= timeStamp)
                        .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));

                try {
                    Thread.sleep(this.period);
                } catch (InterruptedException e) {
                    logger.error("Ftp Observer Interrupeted while in sleep state", e);
                }
            } catch (Exception ex) {
                // continue forever
                logger.error("Observer ftp connection failed:", ex);
            }
        }
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    /**
     * A blocking method to wait on observer object.
     * It is common to register a function to observer object and wait on it.
     */
    public void await() {
        while (this.isAlive()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Interrupeted while in waiting on FtpObserver:", e);
            }
        }
    }

    public FtpManager getFtpManager() {
        return ftpManager;
    }

    public void setFtpManager(FtpManager ftpManager) {
        this.ftpManager = ftpManager;
    }
}
