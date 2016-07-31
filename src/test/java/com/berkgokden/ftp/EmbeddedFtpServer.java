package com.berkgokden.ftp;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.junit.After;
import org.junit.Before;

import java.util.Random;

/**
 * Created by developer on 7/29/16.
 */
public class EmbeddedFtpServer {
    private FtpServer ftpServer;
    protected String homeDirectory;
    protected String server = "localhost";
    protected String username = "test";
    protected String password = "test";
    protected int port = 21;

    @Before
    public void setup() throws Exception {
        if (ftpServer==null || ftpServer.isStopped()) {
            homeDirectory = getClass().getClassLoader()
                    .getResource(".").getPath();

            port = new Random().nextInt(2000) + 1024;

            PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
            UserManager userManager = userManagerFactory.createUserManager();
            BaseUser user = new BaseUser();
            user.setName(username);
            user.setPassword(password);
            user.setHomeDirectory(homeDirectory);
            userManager.save(user);

            ListenerFactory listenerFactory = new ListenerFactory();
            listenerFactory.setPort(port);

            FtpServerFactory factory = new FtpServerFactory();
            factory.setUserManager(userManager);
            factory.addListener("default", listenerFactory.createListener());

            ftpServer = factory.createServer();
            ftpServer.start();
        }
    }
}
