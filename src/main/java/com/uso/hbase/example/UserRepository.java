package com.uso.hbase.example;

import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.repository.HBaseRepository;

import java.util.List;

/**
 * 用户 Repository 接口
 *
 * @author pengchuanjiang
 */
public interface UserRepository extends HBaseRepository<User, String> {

    /**
     * 根据状态查询用户
     */
    default List<User> findByStatus(String status) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("info", "status").equalTo(status)));
    }

    /**
     * 根据年龄范围查询
     */
    default List<User> findByAgeGreaterThan(Integer age) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("info", "age").greaterThan(age)));
    }

    /**
     * 根据年龄范围分页查询
     */
    default Page<User> findByAgeGreaterThan(Integer age, Pageable pageable) {
        return find(com.uso.hbase.query.Query.create()
            .addCriteria(com.uso.hbase.query.Criteria.where("info", "age").greaterThan(age)), pageable);
    }

}
