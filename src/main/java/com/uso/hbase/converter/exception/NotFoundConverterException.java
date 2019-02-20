package com.uso.hbase.converter.exception;

public class NotFoundConverterException extends RuntimeException {

    public NotFoundConverterException(final String message) {
        super(message);
    }

    public NotFoundConverterException(final Class<?> clazz) {
        this("not find the converter of " + clazz.getName());
    }

}
