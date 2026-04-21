package com.uso.hbase.exception;

/**
 * HBase 查询异常
 *
 * @author pengchuanjiang
 */
public class HBaseQueryException extends HBaseException {

    public HBaseQueryException() {
        super();
    }

    public HBaseQueryException(String message) {
        super(message);
    }

    public HBaseQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseQueryException(Throwable cause) {
        super(cause);
    }

}
