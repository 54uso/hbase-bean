package com.uso.hbase.connection;

import com.uso.hbase.config.HBaseProperties;
import com.uso.hbase.exception.HBaseConnectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBase 连接管理器
 *
 * @author pengchuanjiang
 */
public class HBaseConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConnectionManager.class);

    private static final Map<String, Connection> CONNECTION_CACHE = new ConcurrentHashMap<>();

    private static HBaseProperties defaultProperties;

    private HBaseConnectionManager() {
    }

    /**
     * 设置默认配置
     */
    public static void setDefaultProperties(HBaseProperties properties) {
        defaultProperties = properties;
    }

    /**
     * 获取默认连接
     */
    public static Connection getConnection() {
        if (defaultProperties == null) {
            throw new HBaseConnectionException("Default HBaseProperties not configured");
        }
        return getConnection(defaultProperties);
    }

    /**
     * 根据配置获取连接
     */
    public static Connection getConnection(HBaseProperties properties) {
        String cacheKey = buildCacheKey(properties);
        return CONNECTION_CACHE.computeIfAbsent(cacheKey, k -> createConnection(properties));
    }

    /**
     * 根据 ZooKeeper 地址获取连接
     */
    public static Connection getConnection(String quorum) {
        return getConnection(quorum, 2181);
    }

    /**
     * 根据 ZooKeeper 地址和端口获取连接
     */
    public static Connection getConnection(String quorum, int port) {
        HBaseProperties properties = new HBaseProperties(quorum, port);
        return getConnection(properties);
    }

    /**
     * 根据 Configuration 获取连接
     */
    public static Connection getConnection(Configuration configuration) {
        String cacheKey = configuration.get("hbase.zookeeper.quorum", "localhost")
            + ":" + configuration.get("hbase.zookeeper.property.clientPort", "2181");

        return CONNECTION_CACHE.computeIfAbsent(cacheKey, k -> {
            try {
                return ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                throw new HBaseConnectionException("Failed to create HBase connection", e);
            }
        });
    }

    /**
     * 关闭指定连接
     */
    public static void closeConnection(HBaseProperties properties) {
        String cacheKey = buildCacheKey(properties);
        Connection connection = CONNECTION_CACHE.remove(cacheKey);
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("Failed to close HBase connection", e);
            }
        }
    }

    /**
     * 关闭所有连接
     */
    public static void closeAllConnections() {
        for (Map.Entry<String, Connection> entry : CONNECTION_CACHE.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                logger.error("Failed to close HBase connection: {}", entry.getKey(), e);
            }
        }
        CONNECTION_CACHE.clear();
    }

    /**
     * 检查连接是否可用
     */
    public static boolean isConnectionAlive(Connection connection) {
        if (connection == null || connection.isClosed()) {
            return false;
        }
        try {
            connection.getAdmin();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获取缓存的连接数量
     */
    public static int getCachedConnectionCount() {
        return CONNECTION_CACHE.size();
    }

    private static Connection createConnection(HBaseProperties properties) {
        Configuration configuration = createConfiguration(properties);
        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new HBaseConnectionException("Failed to create HBase connection", e);
        }
    }

    private static Configuration createConfiguration(HBaseProperties properties) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", properties.getQuorum());
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(properties.getPort()));
        configuration.setInt("hbase.client.retries.number", properties.getMaxRetries());
        configuration.setInt("hbase.client.operation.timeout", properties.getOperationTimeout());

        if (properties.getTimeout() > 0) {
            configuration.setInt("hbase.zookeeper.session.timeout", properties.getTimeout());
        }

        if (properties.getRootDir() != null && !properties.getRootDir().isEmpty()) {
            configuration.set("hbase.rootdir", properties.getRootDir());
        }

        return configuration;
    }

    private static String buildCacheKey(HBaseProperties properties) {
        return properties.getQuorum() + ":" + properties.getPort();
    }

}
