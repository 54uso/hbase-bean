package com.uso.hbase.spring;

import com.uso.hbase.repository.HBaseRepository;
import com.uso.hbase.repository.SimpleHBaseRepository;
import com.uso.hbase.template.HBaseTemplate;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * HBase Repository Factory Bean
 *
 * @author pengchuanjiang
 */
public class HBaseRepositoryFactoryBean<T extends HBaseRepository<?, ?>> implements FactoryBean<T>, InitializingBean {

    private Class<T> repositoryInterface;

    private HBaseTemplate hbaseTemplate;

    private Class<?> entityClass;

    private T repository;

    @Override
    public T getObject() throws Exception {
        return repository;
    }

    @Override
    public Class<?> getObjectType() {
        return repositoryInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.entityClass == null) {
            this.entityClass = resolveEntityClass();
        }
        if (this.hbaseTemplate == null) {
            throw new IllegalArgumentException("HBaseTemplate must not be null");
        }
        this.repository = (T) new SimpleHBaseRepository<>(hbaseTemplate, entityClass);
    }

    @SuppressWarnings("unchecked")
    private Class<?> resolveEntityClass() {
        if (repositoryInterface == null) {
            // 如果没有 repositoryInterface，尝试从 entityClass 获取
            if (this.entityClass != null) {
                return this.entityClass;
            }
            throw new IllegalArgumentException("Either repositoryInterface or entityClass must be set");
        }

        Type[] genericInterfaces = repositoryInterface.getGenericInterfaces();
        for (Type type : genericInterfaces) {
            if (type instanceof ParameterizedType) {
                ParameterizedType paramType = (ParameterizedType) type;
                if (paramType.getRawType() == HBaseRepository.class) {
                    Type[] typeArgs = paramType.getActualTypeArguments();
                    if (typeArgs.length >= 1 && typeArgs[0] instanceof Class) {
                        return (Class<?>) typeArgs[0];
                    }
                }
            }
        }

        // 递归查找父接口
        Class<?> superClass = repositoryInterface;
        while (superClass != null) {
            Type[] types = superClass.getGenericInterfaces();
            for (Type type : types) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType paramType = (ParameterizedType) type;
                    Type rawType = paramType.getRawType();
                    if (rawType instanceof Class) {
                        Class<?> rawClass = (Class<?>) rawType;
                        if (HBaseRepository.class.isAssignableFrom(rawClass)) {
                            Type[] typeArgs = paramType.getActualTypeArguments();
                            if (typeArgs.length >= 1 && typeArgs[0] instanceof Class) {
                                return (Class<?>) typeArgs[0];
                            }
                        }
                    }
                }
            }
            superClass = superClass.getSuperclass();
        }

        throw new IllegalArgumentException("Could not resolve entity class for repository: " + repositoryInterface.getName());
    }

    public void setRepositoryInterface(Class<T> repositoryInterface) {
        this.repositoryInterface = repositoryInterface;
    }

    public void setHbaseTemplate(HBaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
    }

    public Class<?> getEntityClass() {
        return entityClass;
    }

}
