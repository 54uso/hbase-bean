package com.uso.hbase.converter;

public interface ConverterRegistry {

    void addConverter(Converter<?> converter);

}
