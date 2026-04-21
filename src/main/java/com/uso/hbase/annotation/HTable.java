package com.uso.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase 表注解，指定实体类对应的 HBase 表名
 *
 * @author pengchuanjiang
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface HTable {

    /**
     * 表名
     */
    String value();

    /**
     * 默认列族
     */
    String defaultFamily() default "";

    /**
     * 是否映射所有字段
     */
    boolean allField() default true;

}
