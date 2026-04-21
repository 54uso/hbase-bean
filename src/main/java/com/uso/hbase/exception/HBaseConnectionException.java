package com.uso.hbase.exception;

/**
 * HBase 连接异常
 *
 * @author pengchuanjiang
 */
public class HBaseConnectionException extends HBaseException {

    public HBaseConnectionException() {
        super();
    }

    public HBaseConnectionException(String message) {
        super(message);
    }

    public HBaseConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseConnectionException(Throwable cause) {
        super(cause);
    }

}
