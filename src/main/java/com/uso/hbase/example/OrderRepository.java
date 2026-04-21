package com.uso.hbase.example;

import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.repository.HBaseRepository;

import java.util.List;

/**
 * 订单 Repository 接口
 *
 * @author pengchuanjiang
 */
public interface OrderRepository extends HBaseRepository<Order, String> {

    /**
     * 根据用户ID查询订单
     */
    default List<Order> findByUserId(String userId) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("data", "userId").equalTo(userId)));
    }

    /**
     * 根据状态查询订单
     */
    default List<Order> findByStatus(String status) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("data", "status").equalTo(status)));
    }

    /**
     * 根据金额范围查询订单
     */
    default List<Order> findByAmountBetween(Double minAmount, Double maxAmount) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("data", "amount").greaterThanOrEqualTo(minAmount))
            .addCriteria(com.uso.hbase.query.Criteria.where("data", "amount").lessThanOrEqualTo(maxAmount)));
    }

    /**
     * 根据用户ID分页查询订单
     */
    default Page<Order> findByUserId(String userId, Pageable pageable) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("data", "userId").equalTo(userId)), pageable);
    }

}
