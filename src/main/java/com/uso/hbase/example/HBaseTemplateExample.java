package com.uso.hbase.example;

import com.uso.hbase.converter.DefaultConversionService;
import com.uso.hbase.metadata.TableInfo;
import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.query.Criteria;
import com.uso.hbase.query.Query;
import com.uso.hbase.template.HBaseTemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Arrays;
import java.util.List;

/**
 * HBaseTemplate 使用示例
 *
 * @author pengchuanjiang
 */
public class HBaseTemplateExample {

    public static void main(String[] args) {
        // 1. 创建配置
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // 2. 创建 HBaseTemplate
        HBaseTemplate template = new HBaseTemplate(config, new DefaultConversionService());

        try {
            // ==================== 表操作 ====================
            
            // 检查表是否存在
            boolean tableExists = template.tableExists("user");
            System.out.println("Table 'user' exists: " + tableExists);

            // 创建表
            if (!tableExists) {
                template.createTable("user", "info", "detail");
                System.out.println("Table 'user' created");
            }

            // ==================== 元数据 ====================

            // 获取表信息
            TableInfo userTableInfo = TableInfo.of(User.class);
            System.out.println("Table info: " + userTableInfo);
            System.out.println("Table name: " + userTableInfo.getTableName());
            System.out.println("Rowkey field: " + userTableInfo.getRowkeyColumn().getFieldName());
            System.out.println("Column families: " + userTableInfo.getColumnFamilies());

            // ==================== 保存操作 ====================

            // 保存单条记录
            User user1 = new User("001", "Zhang San", 25, "zhangsan@example.com");
            user1.setAddress("Beijing Chaoyang");
            user1.setStatus("active");
            template.save(user1);
            System.out.println("Saved user: " + user1);

            // 批量保存
            List<User> users = Arrays.asList(
                new User("002", "Li Si", 30, "lisi@example.com", "Shanghai Pudong"),
                new User("003", "Wang Wu", 28, "wangwu@example.com", "Guangzhou Tianhe"),
                new User("004", "Zhao Liu", 35, "zhaoliu@example.com", "Shenzhen Nanshan"),
                new User("005", "Sun Qi", 22, "sunqi@example.com", "Hangzhou Xihu")
            );
            users.get(0).setStatus("active");
            users.get(1).setStatus("inactive");
            users.get(2).setStatus("active");
            users.get(3).setStatus("active");
            template.saveBatch(users);
            System.out.println("Batch saved " + users.size() + " users");

            // ==================== 查询操作 ====================

            // 根据 ID 获取单条记录
            User foundUser = template.get(User.class, "001".getBytes());
            System.out.println("Found user: " + foundUser);

            // 批量获取
            List<User> multiUsers = template.multiGet(User.class, 
                Arrays.asList("001".getBytes(), "002".getBytes(), "003".getBytes()));
            System.out.println("Multi get result count: " + multiUsers.size());

            // 查询所有记录
            List<User> allUsers = template.findAll(User.class);
            System.out.println("All users count: " + allUsers.size());

            // ==================== 条件查询 ====================

            // 大于等于条件查询
            Query ageQuery = Query.create()
                .addCriteria(Criteria.where("info", "age").greaterThanOrEqualTo(28))
                .limit(10);
            List<User> ageFilteredUsers = template.find(User.class, ageQuery);
            System.out.println("Users with age >= 28: " + ageFilteredUsers.size());

            // 等于条件查询
            Query statusQuery = Query.create()
                .addCriteria(Criteria.where("info", "status").equalTo("active"));
            List<User> activeUsers = template.find(User.class, statusQuery);
            System.out.println("Active users: " + activeUsers.size());

            // 包含条件查询
            Query nameQuery = Query.create()
                .addCriteria(Criteria.where("info", "name").contains("Si"));
            List<User> nameFilteredUsers = template.find(User.class, nameQuery);
            System.out.println("Users with name containing 'Si': " + nameFilteredUsers.size());

            // 正则匹配查询
            Query emailQuery = Query.create()
                .addCriteria(Criteria.where("info", "email").matchesRegex(".*@example\\.com"));
            List<User> emailFilteredUsers = template.find(User.class, emailQuery);
            System.out.println("Users with example.com email: " + emailFilteredUsers.size());

            // ==================== 分页查询 ====================

            // 分页查询
            Page<User> firstPage = template.findAll(User.class, Pageable.of(1, 2));
            System.out.println("First page - PageNum: " + firstPage.getPageNum()
                + ", PageSize: " + firstPage.getPageSize()
                + ", Total: " + firstPage.getTotalElements()
                + ", TotalPages: " + firstPage.getTotalPages()
                + ", HasNext: " + firstPage.hasNext()
                + ", Content: " + firstPage.getContent());

            Page<User> secondPage = template.findAll(User.class, Pageable.of(2, 2));
            System.out.println("Second page content: " + secondPage.getContent());

            // 带条件的分页查询
            Query pageQuery = Query.create()
                .addCriteria(Criteria.where("info", "age").greaterThan(20));
            Page<User> conditionalPage = template.find(User.class, pageQuery, Pageable.of(1, 3));
            System.out.println("Conditional page: " + conditionalPage);

            // ==================== 统计和存在性检查 ====================

            // 统计总数
            long totalCount = template.countAll(User.class);
            System.out.println("Total user count: " + totalCount);

            // 条件统计
            Query countQuery = Query.create()
                .addCriteria(Criteria.where("info", "status").equalTo("active"));
            long activeCount = template.count(User.class, countQuery);
            System.out.println("Active user count: " + activeCount);

            // 检查是否存在
            boolean exists = template.exists(User.class, "001".getBytes());
            System.out.println("User 001 exists: " + exists);

            // 条件存在性检查
            Query existsQuery = Query.create()
                .addCriteria(Criteria.where("info", "email").equalTo("zhangsan@example.com"));
            boolean emailExists = template.exists(User.class, existsQuery);
            System.out.println("User with email exists: " + emailExists);

            // ==================== 更新操作 ====================

            // 更新记录
            User updateUser = template.get(User.class, "001".getBytes());
            if (updateUser != null) {
                updateUser.setName("Zhang San Updated");
                updateUser.setAge(26);
                template.save(updateUser);
                System.out.println("Updated user: " + updateUser);
            }

            // ==================== 删除操作 ====================

            // 删除单条记录
            template.delete(User.class, "005".getBytes());
            System.out.println("Deleted user 005");

            // 批量删除
            template.deleteBatch(User.class, Arrays.asList("002".getBytes(), "003".getBytes()));
            System.out.println("Deleted users 002 and 003");

            // 条件删除
            Query deleteQuery = Query.create()
                .addCriteria(Criteria.where("info", "status").equalTo("inactive"));
            template.deleteByQuery(User.class, deleteQuery);
            System.out.println("Deleted inactive users");

            // 验证删除结果
            List<User> remainingUsers = template.findAll(User.class);
            System.out.println("Remaining users: " + remainingUsers);

            // ==================== 清理 ====================

            // 删除表
            // template.dropTable("user");
            // System.out.println("Table 'user' dropped");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            template.close();
            System.out.println("Template closed");
        }
    }

}
