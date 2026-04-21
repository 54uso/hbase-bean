package com.uso.hbase.config;

import java.io.Serializable;

/**
 * HBase 配置属性
 *
 * @author pengchuanjiang
 */
public class HBaseProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * ZooKeeper 地址
     */
    private String quorum = "localhost";

    /**
     * ZooKeeper 端口
     */
    private int port = 2181;

    /**
     * ZooKeeper 超时时间(毫秒)
     */
    private int timeout = 60000;

    /**
     * HBase 根目录
     */
    private String rootDir;

    /**
     * 是否启用 Kerberos 认证
     */
    private boolean kerberosEnabled = false;

    /**
     * Kerberos principal
     */
    private String kerberosPrincipal;

    /**
     * Kerberos keytab 文件路径
     */
    private String kerberosKeytab;

    /**
     * 最大重试次数
     */
    private int maxRetries = 3;

    /**
     * 客户端操作超时时间(毫秒)
     */
    private int operationTimeout = 30000;

    /**
     * Scanner 缓存大小
     */
    private int scannerCaching = 1000;

    public HBaseProperties() {
    }

    public HBaseProperties(String quorum, int port) {
        this.quorum = quorum;
        this.port = port;
    }

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public boolean isKerberosEnabled() {
        return kerberosEnabled;
    }

    public void setKerberosEnabled(boolean kerberosEnabled) {
        this.kerberosEnabled = kerberosEnabled;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    public String getKerberosKeytab() {
        return kerberosKeytab;
    }

    public void setKerberosKeytab(String kerberosKeytab) {
        this.kerberosKeytab = kerberosKeytab;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public int getScannerCaching() {
        return scannerCaching;
    }

    public void setScannerCaching(int scannerCaching) {
        this.scannerCaching = scannerCaching;
    }

    @Override
    public String toString() {
        return "HBaseProperties{" +
            "quorum='" + quorum + '\'' +
            ", port=" + port +
            ", timeout=" + timeout +
            ", kerberosEnabled=" + kerberosEnabled +
            '}';
    }

}
