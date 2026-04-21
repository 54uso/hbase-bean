package com.uso.hbase.template;

import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.query.Query;

import java.util.List;

/**
 * HBase 操作接口
 *
 * @author pengchuanjiang
 */
public interface HBaseOperations {

    /**
     * 根据 rowkey 获取单条记录
     */
    <T> T get(Class<T> entityClass, byte[] rowkey);

    /**
     * 根据多个 rowkey 批量获取记录
     */
    <T> List<T> multiGet(Class<T> entityClass, List<byte[]> rowkeys);

    /**
     * 保存单条记录
     */
    <T> void save(T entity);

    /**
     * 批量保存记录
     */
    <T> void saveBatch(List<T> entities);

    /**
     * 更新记录
     */
    <T> boolean update(T entity);

    /**
     * 根据 rowkey 删除记录
     */
    <T> void delete(Class<T> entityClass, byte[] rowkey);

    /**
     * 批量删除记录
     */
    <T> void deleteBatch(Class<T> entityClass, List<byte[]> rowkeys);

    /**
     * 根据实体对象删除
     */
    <T> void deleteByEntity(T entity);

    /**
     * 删除所有记录
     */
    <T> void deleteAll(Class<T> entityClass);

    /**
     * 条件删除
     */
    <T> void deleteByQuery(Class<T> entityClass, Query query);

    /**
     * 条件查询
     */
    <T> List<T> find(Class<T> entityClass, Query query);

    /**
     * 分页查询
     */
    <T> Page<T> find(Class<T> entityClass, Query query, Pageable pageable);

    /**
     * 查询单条记录
     */
    <T> T findOne(Class<T> entityClass, Query query);

    /**
     * 查询所有记录
     */
    <T> List<T> findAll(Class<T> entityClass);

    /**
     * 分页查询所有记录
     */
    <T> Page<T> findAll(Class<T> entityClass, Pageable pageable);

    /**
     * 统计记录数
     */
    <T> long count(Class<T> entityClass, Query query);

    /**
     * 统计所有记录数
     */
    <T> long countAll(Class<T> entityClass);

    /**
     * 检查记录是否存在
     */
    <T> boolean exists(Class<T> entityClass, byte[] rowkey);

    /**
     * 根据条件检查记录是否存在
     */
    <T> boolean exists(Class<T> entityClass, Query query);

    /**
     * 执行回调
     */
    <T> T execute(TableCallback<T> action);

    /**
     * 执行扫描回调
     */
    <T> T executeScan(Class<?> entityClass, ScanCallback<T> callback);

    /**
     * 表是否存在
     */
    boolean tableExists(String tableName);

    /**
     * 创建表
     */
    void createTable(String tableName, String... columnFamilies);

    /**
     * 删除表
     */
    void dropTable(String tableName);

    /**
     * 禁用表
     */
    void disableTable(String tableName);

    /**
     * 启用表
     */
    void enableTable(String tableName);

}
