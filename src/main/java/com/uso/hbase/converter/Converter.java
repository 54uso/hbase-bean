package com.uso.hbase.converter;

public interface Converter<T> {

    byte[] convert(T source);

    T from(byte[] bytes);

}
