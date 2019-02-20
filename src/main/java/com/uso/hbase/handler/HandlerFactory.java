package com.uso.hbase.handler;

import com.uso.hbase.handler.exception.HProcessException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface HandlerFactory {

    <T> T convert(Result result, Class<T> clazz) throws HProcessException;

    Put buildPut(Object source) throws HProcessException;

}
