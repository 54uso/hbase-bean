# HBase Bean - MyBatis 风格的 HBase ORM 框架

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8+-orange.svg)]()

一个基于注解的 HBase ORM 框架，借鉴了 MyBatis 的设计理念，提供了简洁、易用的 API 来操作 HBase。

## 特性

- 🎯 **注解驱动** - 使用注解定义实体类与 HBase 表的映射关系
- 🔄 **自动类型转换** - 内置多种类型转换器，支持自动装箱/拆箱
- 📦 **Repository 模式** - 提供类似 Spring Data JPA 的 Repository 接口
- 🔍 **链式查询构建** - 使用 Query 和 Criteria 构建复杂查询条件
- 📄 **分页支持** - 内置分页查询功能
- 🚀 **Spring 集成** - 支持 Spring 框架无缝集成
- 🛡️ **完善的异常体系** - 细化的异常类型，便于问题定位

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>com.uso</groupId>
    <artifactId>hbase-bean</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### 2. 定义实体类

```java
@HTable(value = "user", defaultFamily = "info")
public class User {

    @HRowkey
    private String id;

    @HColumn(qualifier = "name")
    private String name;

    @HColumn(qualifier = "age")
    private Integer age;

    @HColumn(qualifier = "email")
    private String email;

    @HColumn(family = "detail", qualifier = "address")
    private String address;

    @HTimestamp
    private Long timestamp;

    // getter/setter 省略...
}
```

### 3. 使用 HBaseTemplate

```java
// 创建配置
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost");

// 创建模板
HBaseTemplate template = new HBaseTemplate(config);

// 保存记录
User user = new User("001", "张三", 25, "zhangsan@example.com");
template.save(user);

// 查询记录
User foundUser = template.get(User.class, "001".getBytes());

// 条件查询
Query query = Query.create()
    .addCriteria(Criteria.where("info", "age").greaterThan(20))
    .limit(10);
List<User> users = template.find(User.class, query);

// 分页查询
Page<User> page = template.find(User.class, Query.create(), Pageable.of(1, 10));

// 删除记录
template.delete(User.class, "001".getBytes());
```

### 4. 使用 Repository

```java
// 定义 Repository 接口
public interface UserRepository extends HBaseRepository<User, String> {
}

// 使用 (需要 Spring 集成)
@Autowired
private UserRepository userRepository;

// CRUD 操作
userRepository.save(user);
User user = userRepository.findById("001");
List<User> users = userRepository.findAll();
userRepository.deleteById("001");
```

### 5. Spring 集成

```java
@Configuration
@EnableHBaseRepositories(basePackages = "com.example.repository")
public class HBaseConfig {

    @Bean
    public HBaseTemplate hbaseTemplate() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        return new HBaseTemplate(config);
    }
}
```

## 注解说明

| 注解 | 位置 | 说明 |
|------|------|------|
| `@HTable` | 类 | 定义表名和默认列族 |
| `@HRow` | 类 | 兼容旧版，推荐使用 @HTable |
| `@HRowkey` | 字段 | 标识 rowkey 字段 |
| `@HColumn` | 字段 | 定义列族和列限定符 |
| `@HTimestamp` | 字段 | 映射 HBase 时间戳 |
| `@HVersion` | 字段 | 映射版本号 |
| `@HIgnore` | 字段 | 忽略该字段 |

## 查询构建

### Query 构建器

```java
Query query = Query.create()
    .startRow("row1".getBytes())
    .stopRow("row100".getBytes())
    .addCriteria(Criteria.where("info", "age").greaterThan(18))
    .addCriteria(Criteria.where("info", "status").equalTo("active"))
    .orderBy("info", "createTime", Order.DESC)
    .limit(20)
    .offset(10);
```

### Criteria 条件

```java
Criteria.where("family", "qualifier")
    .equalTo(value)           // 等于
    .notEqualTo(value)        // 不等于
    .greaterThan(value)       // 大于
    .greaterThanOrEqualTo(value) // 大于等于
    .lessThan(value)          // 小于
    .lessThanOrEqualTo(value) // 小于等于
    .contains("str")          // 包含子串
    .matchesRegex("pattern")  // 正则匹配
```

## 类型转换器

内置支持以下类型的自动转换：

- `String`
- `Integer` / `int`
- `Long` / `long`
- `Float` / `float`
- `Double` / `double`
- `Boolean` / `boolean`
- `Short` / `short`
- `Byte` / `byte`
- `Character` / `char`
- `BigDecimal`
- `ByteBuffer`
- `byte[]`
- `Date`
- `Enum` (枚举类型)

## 异常体系

```
HBaseException (基础异常)
├── HBaseConnectionException (连接异常)
├── HBaseQueryException (查询异常)
├── HBaseWriteException (写入异常)
├── HBaseMetadataException (元数据异常)
└── ConversionException (转换异常)
```

## 配置项

### HBaseTemplate 配置

```java
// 自定义转换器
ConversionService conversionService = new DefaultConversionService();
HBaseTemplate template = new HBaseTemplate(config, conversionService);
```

### HBase 连接配置

```properties
# ZooKeeper 配置
hbase.zookeeper.quorum=localhost
hbase.zookeeper.property.clientPort=2181

# HBase 配置
hbase.rootdir=hdfs://localhost:9000/hbase
```

## 最佳实践

1. **实体类设计**
   - 使用 `@HTable` 注解指定表名和默认列族
   - 每个实体类必须有一个 `@HRowkey` 字段
   - 使用 `@HIgnore` 忽略不需要映射的字段

2. **批量操作**
   - 批量保存时建议每批不超过 1000 条
   - 使用 `saveBatch()` 方法提高性能

3. **查询优化**
   - 尽量使用 rowkey 范围查询
   - 避免全表扫描
   - 使用分页避免一次加载过多数据

4. **连接管理**
   - HBaseTemplate 应该作为单例使用
   - 使用完毕后调用 `close()` 方法释放资源

## 示例代码

更多示例请参考 `com.uso.hbase.example` 包。

## License

MIT License
