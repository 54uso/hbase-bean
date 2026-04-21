package com.uso.hbase.exception;

/**
 * 转换异常
 *
 * @author pengchuanjiang
 */
public class ConversionException extends HBaseException {

    public ConversionException() {
        super();
    }

    public ConversionException(String message) {
        super(message);
    }

    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConversionException(Throwable cause) {
        super(cause);
    }

}
