package org.kk.cheetah.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.kk.cheetah.config.ServerConfig;
import org.kk.cheetah.zookeeper.serializer.DefaultZKSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMetadataHandler {
    private static Logger logger = LoggerFactory.getLogger(ZKMetadataHandler.class);

    private static ZkClient zkc;

    private static final int connectionTimeout = 100000;
    public static String cheetahZKPrefix;

    public static void init() {
        Properties properties = new Properties();
        InputStream is = null;
        if ((is = ServerConfig.class.getClassLoader().getResourceAsStream("zookeeper.properties")) == null) {
            throw new NullPointerException("zookeeper.properties不存在");
        }
        try {
            properties.load(is);
        } catch (IOException e) {
            logger.error("ZKMetadataHandler", e);
        }
        String cluster = properties.getProperty("cluster");
        if (cluster == null) {
            throw new NullPointerException("必须指定cluster");
        }
        zkc = new ZkClient(new ZkConnection(cluster),
                connectionTimeout);
        zkc.setZkSerializer(new DefaultZKSerializer());
        cheetahZKPrefix = properties.getProperty("cheetahZKPrefix", "/cheetah");
    }

    public static void createEphemeral(String key, Object value) {
        zkc.createEphemeral(key, value);
    }

    public static void createPersistent(String key, Object value) {
        if (!zkc.exists(key)) {
            zkc.createPersistent(key, true);
        }
        zkc.writeData(key, value);
    }

    public static void update(String key, Object value) {
        zkc.writeData(key, value);
    }

    public static <T> T getData(String key) {
        return zkc.readData(key, true);
    }

}
