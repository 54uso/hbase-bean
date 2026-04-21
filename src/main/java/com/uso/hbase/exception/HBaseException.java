package com.uso.hbase.exception;

/**
 * HBase 操作基础异常
 *
 * @author pengchuanjiang
 */
public class HBaseException extends RuntimeException {

    public HBaseException() {
        super();
    }

    public HBaseException(String message) {
        super(message);
    }

    public HBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseException(Throwable cause) {
        super(cause);
    }

}
