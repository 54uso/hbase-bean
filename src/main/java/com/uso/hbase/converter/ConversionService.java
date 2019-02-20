package com.uso.hbase.converter;

public interface ConversionService {

    <T> byte[] convert(T source);

    <T> T from(byte[] bytes, Class<T> type);

}
