package com.uso.hbase.exception;

/**
 * HBase 元数据异常
 *
 * @author pengchuanjiang
 */
public class HBaseMetadataException extends HBaseException {

    public HBaseMetadataException() {
        super();
    }

    public HBaseMetadataException(String message) {
        super(message);
    }

    public HBaseMetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseMetadataException(Throwable cause) {
        super(cause);
    }

}
