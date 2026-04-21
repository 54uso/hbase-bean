package com.uso.hbase.template;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Scan 执行回调接口
 *
 * @author pengchuanjiang
 */
public interface ScanCallback<T> {

    /**
     * 在 ResultScanner 上执行操作
     *
     * @param scan 扫描对象
     * @param scanner 结果扫描器
     * @return 执行结果
     */
    T doInScan(Scan scan, ResultScanner scanner) throws IOException;

}
