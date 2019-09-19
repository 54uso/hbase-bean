#### 1. 注解使用
```java
@HRow
public class Model {

    /**
     * rowkey.
     */
    @HRowkey
    private String row;
    
    /**
     * 对应的列.
     */
    @HColumn(family = "B", qualifier = "URL")
    private String url;
    
}
```
#### 2. 创建转换对象
```java
@Configuration
public class AppConfiguration {

    @Bean
    public HandlerFactory handlerFactory() {
        return new DefaultHandlerFactory();
    }

}
```
#### 3.使用
```java
@Service
public class AppService {

    @Autowired
    private HandlerFactory handlerFactory;
    
    /**
     * hbase的Result对象转java对象.
     */
    public <T> T convert(final Result result, Class<T> clazz) throws HProcessException {
        return this.handlerFactory.convert(result, clazz);
    }

   /**
    * java对象转hbase的Put对象.
    */
    public Put buildPut(final Object source) throws HProcessException {
        return this.handlerFactory.buildPut(source);
    }

}
```
