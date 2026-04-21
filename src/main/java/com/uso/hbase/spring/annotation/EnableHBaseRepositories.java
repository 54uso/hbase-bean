package com.uso.hbase.spring.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 启用 HBase Repository 功能
 *
 * @author pengchuanjiang
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(HBaseRepositoriesRegistrar.class)
public @interface EnableHBaseRepositories {

    /**
     * Repository 扫描的包路径
     */
    String[] basePackages() default {};

    /**
     * Repository 扫描的包类
     */
    Class<?>[] basePackageClasses() default {};

}
