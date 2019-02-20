package com.uso.hbase.handler.exception;

public class NotFoundRowkeyException extends RuntimeException {

    public NotFoundRowkeyException(final String message) {
        super(message);
    }

    public NotFoundRowkeyException(final Class<?> clazz) {
        this("not found rowkey in class " + clazz.getName());
    }

}
