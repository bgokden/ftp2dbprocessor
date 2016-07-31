package com.berkgokden.ftp;

import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by developer on 7/29/16.
 */
public class FtpObserverTest extends EmbeddedFtpServer {

    @Test
    public void shouldPassWhenNewFilesProcessed() throws Exception {
        FtpManager ftpManager = FtpManager.getInstance();
        ftpManager.setServer(server);
        ftpManager.setUser(username);
        ftpManager.setPassword(password);
        ftpManager.setPort(port);
        String folder = "";
        long period = 1000;
        long timeStamp = 0;
        int numberOfFiles = 10;
        Set<String> files = new HashSet<>(numberOfFiles);
        FtpObserver ftpObserver = new FtpObserver(ftpManager, folder, period, timeStamp,
                ftpFile -> {
                    files.add(ftpFile.getName());
                });
        ftpObserver.start();

        Thread.sleep(numberOfFiles*period);
        for (int i = 0; i < numberOfFiles; i++) {
            File ftpHome = new File(homeDirectory+i+".testfile");
            ftpHome.createNewFile();
            Thread.sleep(period);
        }

        Thread.sleep(numberOfFiles*period);
        for (int i = 0; i < numberOfFiles; i++) {
            assertTrue(files.contains(i+".testfile"));
            File ftpHome = new File(homeDirectory+i+".testfile");
            ftpHome.delete();
        }
    }
}