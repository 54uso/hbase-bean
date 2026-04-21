package com.uso.hbase.spring.annotation;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import com.uso.hbase.repository.HBaseRepository;
import com.uso.hbase.spring.HBaseRepositoryFactoryBean;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * HBase Repository 注册器
 *
 * @author pengchuanjiang
 */
public class HBaseRepositoriesRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(
            importingClassMetadata.getAnnotationAttributes(EnableHBaseRepositories.class.getName()));

        if (attributes == null) {
            return;
        }

        // 获取扫描包路径
        Set<String> basePackages = getBasePackages(attributes, importingClassMetadata);

        // 扫描并注册 Repository
        for (String basePackage : basePackages) {
            registerRepositoriesInPackage(basePackage, registry);
        }
    }

    private Set<String> getBasePackages(AnnotationAttributes attributes, AnnotationMetadata metadata) {
        Set<String> basePackages = new LinkedHashSet<>();

        // 从 basePackages 属性获取
        String[] packages = attributes.getStringArray("basePackages");
        for (String pkg : packages) {
            if (pkg != null && !pkg.isEmpty()) {
                basePackages.add(pkg);
            }
        }

        // 从 basePackageClasses 属性获取
        Class<?>[] basePackageClasses = attributes.getClassArray("basePackageClasses");
        for (Class<?> clazz : basePackageClasses) {
            basePackages.add(clazz.getPackage().getName());
        }

        // 如果没有指定包路径，使用注解所在类的包路径
        if (basePackages.isEmpty()) {
            String className = metadata.getClassName();
            try {
                Class<?> clazz = Class.forName(className);
                basePackages.add(clazz.getPackage().getName());
            } catch (ClassNotFoundException e) {
                // 使用类名推断包名
                int lastDotIndex = className.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    basePackages.add(className.substring(0, lastDotIndex));
                }
            }
        }

        return basePackages;
    }

    private void registerRepositoriesInPackage(String basePackage, BeanDefinitionRegistry registry) {
        // 使用 Spring 的 ClassPathScanningCandidateComponentProvider 扫描
        org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider scanner =
            new org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider(false);

        scanner.addIncludeFilter(new AnnotationTypeFilter(com.uso.hbase.annotation.HTable.class));
        scanner.addIncludeFilter(new AnnotationTypeFilter(com.uso.hbase.annotation.HRow.class));

        // 扫描候选组件
        Set<org.springframework.beans.factory.config.BeanDefinition> candidateComponents = scanner.findCandidateComponents(basePackage);

        for (org.springframework.beans.factory.config.BeanDefinition beanDefinition : candidateComponents) {
            try {
                String className = beanDefinition.getBeanClassName();
                if (className != null) {
                    Class<?> entityClass = Class.forName(className);

                    // 注册 Repository Bean
                    String beanName = getRepositoryBeanName(entityClass);
                    if (!registry.containsBeanDefinition(beanName)) {
                        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(HBaseRepositoryFactoryBean.class);
                        builder.addPropertyValue("entityClass", entityClass);
                        registry.registerBeanDefinition(beanName, builder.getBeanDefinition());
                    }
                }
            } catch (ClassNotFoundException e) {
                // 忽略无法加载的类
            }
        }
    }

    private String getRepositoryBeanName(Class<?> entityClass) {
        String simpleName = entityClass.getSimpleName();
        return Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1) + "Repository";
    }

}
