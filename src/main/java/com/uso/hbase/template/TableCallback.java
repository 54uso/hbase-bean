package com.uso.hbase.template;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Table 操作回调接口
 *
 * @author pengchuanjiang
 */
public interface TableCallback<T> {

    /**
     * 在 Table 上执行操作
     */
    T doInTable(Connection connection) throws IOException;

}
