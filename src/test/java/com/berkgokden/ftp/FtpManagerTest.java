package com.berkgokden.ftp;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

/**
 * Created by developer on 7/25/16.
 */
public class FtpManagerTest extends EmbeddedFtpServer {

    @Test
    public void shouldPassWhenFileDownloadedCorrectly() throws Exception {
        FtpManager ftpManager = FtpManager.getInstance();
        ftpManager.setServer(server);
        ftpManager.setUser(username);
        ftpManager.setPassword(password);
        ftpManager.setPort(port);

        String expectedFilePath = getClass().getClassLoader()
                .getResource("file1.csv").getPath();
        byte[] expectedFileData = IOUtils
                .toByteArray(new FileInputStream(expectedFilePath));
        assertNotNull(expectedFileData);

        byte[] fileData = ftpManager.getFile("file1.csv");
        assertNotNull(fileData);

        assertArrayEquals(expectedFileData, fileData);
    }

    @Test
    public void shouldPassWhenFileUncompressedCorrectly() throws Exception {
        String compressFilePath = getClass().getClassLoader()
                .getResource("file1.csv.bz2").getPath();
        String uncompressFilePath = getClass().getClassLoader()
                .getResource("file1.csv").getPath();
        byte[] compressedData = IOUtils
                .toByteArray(new FileInputStream(compressFilePath));
        byte[] expectedUncompressedData = IOUtils
                .toByteArray(new FileInputStream(uncompressFilePath));
        byte[] uncompressedData = FtpManager.uncompress(compressedData);
        assertArrayEquals(expectedUncompressedData, uncompressedData);
    }
}