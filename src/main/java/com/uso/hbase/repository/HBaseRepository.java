package com.uso.hbase.repository;

import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.query.Query;

import java.util.List;

/**
 * HBase Repository 基础接口，类似 Spring Data JPA 的 CrudRepository
 *
 * @author pengchuanjiang
 */
public interface HBaseRepository<T, ID> {

    /**
     * 根据 ID 获取实体
     *
     * @param id 行键
     * @return 实体对象，不存在返回 null
     */
    T findById(ID id);

    /**
     * 批量获取
     *
     * @param ids 行键集合
     * @return 实体列表
     */
    List<T> findAllById(Iterable<ID> ids);

    /**
     * 保存实体
     *
     * @param entity 实体对象
     * @return 保存后的实体
     */
    <S extends T> S save(S entity);

    /**
     * 批量保存
     *
     * @param entities 实体集合
     * @return 保存后的实体集合
     */
    <S extends T> Iterable<S> saveAll(Iterable<S> entities);

    /**
     * 检查是否存在
     *
     * @param id 行键
     * @return 是否存在
     */
    boolean existsById(ID id);

    /**
     * 查询所有
     *
     * @return 所有实体列表
     */
    List<T> findAll();

    /**
     * 分页查询所有
     *
     * @param pageable 分页参数
     * @return 分页结果
     */
    Page<T> findAll(Pageable pageable);

    /**
     * 统计总数
     *
     * @return 总记录数
     */
    long count();

    /**
     * 根据 ID 删除
     *
     * @param id 行键
     */
    void deleteById(ID id);

    /**
     * 删除实体
     *
     * @param entity 实体对象
     */
    void delete(T entity);

    /**
     * 删除所有
     *
     * @param entities 实体集合
     */
    void deleteAll(Iterable<? extends T> entities);

    /**
     * 条件查询
     *
     * @param query 查询条件
     * @return 实体列表
     */
    List<T> find(Query query);

    /**
     * 分页条件查询
     *
     * @param query 查询条件
     * @param pageable 分页参数
     * @return 分页结果
     */
    Page<T> find(Query query, Pageable pageable);

    /**
     * 查询单条
     *
     * @param query 查询条件
     * @return 实体对象
     */
    T findOne(Query query);

    /**
     * 条件统计
     *
     * @param query 查询条件
     * @return 记录数
     */
    long count(Query query);

}
