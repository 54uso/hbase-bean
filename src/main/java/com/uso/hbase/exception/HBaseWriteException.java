package com.uso.hbase.exception;

/**
 * HBase 写入异常
 *
 * @author pengchuanjiang
 */
public class HBaseWriteException extends HBaseException {

    public HBaseWriteException() {
        super();
    }

    public HBaseWriteException(String message) {
        super(message);
    }

    public HBaseWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseWriteException(Throwable cause) {
        super(cause);
    }

}
