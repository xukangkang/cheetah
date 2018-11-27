package org.kk.cheetah.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.kk.cheetah.handler.ProducerRecordRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerConfig {
    private static Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);
    public static String dataFilePath;

    public static int backlog;

    public static void init() throws IOException {
        Properties properties = new Properties();
        InputStream is = null;
        if ((is = ServerConfig.class.getClassLoader().getResourceAsStream("server.properties")) == null) {
            throw new NullPointerException("server.properties不存在");
        }
        properties.load(is);
        backlog = Integer.valueOf(properties.getProperty("backlog", "1024"));
        if ((dataFilePath = properties.getProperty("dataFilePath")) == null) {
            //TODO 自定义异常
            throw new RuntimeException("dataFilePath不能为null");
        }
        File file = new File(dataFilePath);
        if (!file.exists()) {
            file.mkdirs();
        }
    }
}
