package com.berkgokden.ftp;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.net.ftp.*;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * A helper class to hold Ftp Server related objects and methods
 * This class designed as Singleton
 * since there can be a single FtpManager instance
 * in this project
 */
public class FtpManager {
    private static final Logger logger = Logger.getLogger(FtpManager.class.getName());

    private static FtpManager ourInstance = new FtpManager();

    public static FtpManager getInstance() {
        return ourInstance;
    }

    private String server;
    private String user;
    private String password;
    private int port = 21;
    private FTPClient ftp;

    private FtpManager() {
        ftp = new FTPClient();
    }

    /**
     * This method checks and created connection when needed.
     * @throws IOException
     */
    public void connect() throws IOException {
        if (!ftp.isConnected()) {
            logger.info("Connect to "+server+":"+port);
            ftp.connect(server, port);
            ftp.login(user, password);
            ftp.enterLocalPassiveMode();
        }
    }

    /**
     * Disconnect method can be used to retrigger connect method
     * after a server, user, password change
     * @throws IOException
     */
    public void disconnect() throws IOException {
        ftp.disconnect();
    }

    /**
     * This method returns current ftpClient and connect automatically
     * @return current ftpClient instance
     * @throws IOException
     */
    public FTPClient getFtp() throws IOException {
        connect();
        return ftp;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Download a file from FtpServer
     * @param filename path of file in the remote server
     * @return file content as byte[]
     */
    public byte[] getFile(String filename) {
        logger.info("File will be downloaded: "+filename);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            this.getFtp().retrieveFile(filename, stream);
            return stream.toByteArray();
        } catch (IOException e) {
            logger.error("Ftp File Download Failed:", e);
        }
        return null;
    }

    /**
     * A helper function to uncompress bz2 compressed byte[]
     * TODO: this method is not directly ftp related move to another class
     *
     * @param data compressed bz2 data as byte[]
     * @return uncpressed data as byte[]
     * @throws IOException
     */
    public static byte[] uncompress(byte[] data) throws IOException {
        InputStream fin = new ByteArrayInputStream(data);
        BufferedInputStream in = new BufferedInputStream(fin);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
        final byte[] buffer = new byte[1024];
        int n = 0;
        while (-1 != (n = bzIn.read(buffer))) {
            out.write(buffer, 0, n);
        }
        out.close();
        bzIn.close();
        return out.toByteArray();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
