package com.uso.hbase.metadata;

import com.uso.hbase.annotation.*;
import com.uso.hbase.exception.HBaseMetadataException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表信息元数据管理
 *
 * @author pengchuanjiang
 */
public class TableInfo {

    private static final Map<Class<?>, TableInfo> TABLE_INFO_CACHE = new ConcurrentHashMap<>();

    private Class<?> entityClass;

    private String tableName;

    private String defaultFamily;

    private boolean allField;

    private ColumnInfo rowkeyColumn;

    private ColumnInfo timestampColumn;

    private ColumnInfo versionColumn;

    private List<ColumnInfo> columnList;

    private TableInfo(Class<?> entityClass) {
        this.entityClass = entityClass;
        this.columnList = new ArrayList<>();
        this.initTableInfo();
    }

    /**
     * 获取表信息
     */
    public static TableInfo of(Class<?> entityClass) {
        return TABLE_INFO_CACHE.computeIfAbsent(entityClass, TableInfo::new);
    }

    /**
     * 清除缓存
     */
    public static void clearCache() {
        TABLE_INFO_CACHE.clear();
    }

    /**
     * 移除指定类的缓存
     */
    public static void removeCache(Class<?> entityClass) {
        TABLE_INFO_CACHE.remove(entityClass);
    }

    /**
     * 获取缓存数量
     */
    public static int getCacheSize() {
        return TABLE_INFO_CACHE.size();
    }

    /**
     * 检查类是否有 @HTable 或 @HRow 注解
     */
    public static boolean isEntityClass(Class<?> clazz) {
        return clazz.getAnnotation(HTable.class) != null 
            || clazz.getAnnotation(HRow.class) != null;
    }

    private void initTableInfo() {
        HTable hTable = entityClass.getAnnotation(HTable.class);
        HRow hRow = entityClass.getAnnotation(HRow.class);

        if (hTable != null) {
            this.tableName = hTable.value();
            this.defaultFamily = hTable.defaultFamily();
            this.allField = hTable.allField();
        } else if (hRow != null) {
            this.tableName = entityClass.getSimpleName();
            this.defaultFamily = hRow.defaultFamily();
            this.allField = hRow.allField();
        } else {
            throw new HBaseMetadataException("Class " + entityClass.getName() 
                + " must be annotated with @HTable or @HRow");
        }

        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.getAnnotation(HIgnore.class) != null) {
                continue;
            }
            ColumnInfo columnInfo = buildColumnInfo(field);
            if (columnInfo != null) {
                if (columnInfo.isRowkey()) {
                    this.rowkeyColumn = columnInfo;
                } else if (columnInfo.isTimestamp()) {
                    this.timestampColumn = columnInfo;
                } else if (columnInfo.isVersion()) {
                    this.versionColumn = columnInfo;
                }
                this.columnList.add(columnInfo);
            }
        }

        // 检查是否有 rowkey
        if (this.rowkeyColumn == null) {
            throw new HBaseMetadataException("Entity class " + entityClass.getName() 
                + " must have a field annotated with @HRowkey");
        }
    }

    private ColumnInfo buildColumnInfo(Field field) {
        HRowkey hRowkey = field.getAnnotation(HRowkey.class);
        HTimestamp hTimestamp = field.getAnnotation(HTimestamp.class);
        HVersion hVersion = field.getAnnotation(HVersion.class);
        HColumn hColumn = field.getAnnotation(HColumn.class);

        if (hRowkey == null && hColumn == null && hTimestamp == null && hVersion == null && !this.allField) {
            return null;
        }

        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.setFieldName(field.getName());
        columnInfo.setFieldType(field.getType());
        columnInfo.setField(field);

        try {
            columnInfo.setGetMethod(buildGetMethod(entityClass, field));
            columnInfo.setSetMethod(buildSetMethod(entityClass, field));
        } catch (NoSuchMethodException e) {
            throw new HBaseMetadataException("Cannot find getter/setter for field: " + field.getName(), e);
        }

        if (hRowkey != null) {
            columnInfo.setRowkey(true);
            return columnInfo;
        }

        if (hTimestamp != null) {
            if (field.getType() != Long.class && field.getType() != long.class) {
                throw new HBaseMetadataException("@HTimestamp field must be Long or long type: " + field.getName());
            }
            columnInfo.setTimestamp(true);
            return columnInfo;
        }

        if (hVersion != null) {
            columnInfo.setVersion(true);
            return columnInfo;
        }

        String family = null;
        if (hColumn != null && !isBlank(hColumn.family())) {
            family = hColumn.family();
        } else if (!isBlank(this.defaultFamily)) {
            family = this.defaultFamily;
        }

        if (isBlank(family)) {
            throw new HBaseMetadataException("Column family not specified for field: " + field.getName());
        }

        String qualifier = field.getName();
        if (hColumn != null && !isBlank(hColumn.qualifier())) {
            qualifier = hColumn.qualifier();
        }

        columnInfo.setFamily(family);
        columnInfo.setQualifier(qualifier);
        return columnInfo;
    }

    private Method buildGetMethod(Class<?> clazz, Field field) throws NoSuchMethodException {
        String name = field.getName();
        if (isBooleanType(field.getType())) {
            name = name.replaceAll("^is", "");
        }
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        String methodName = "get" + name;
        if (isBooleanType(field.getType())) {
            methodName = "is" + name;
        }
        return clazz.getDeclaredMethod(methodName);
    }

    private Method buildSetMethod(Class<?> clazz, Field field) throws NoSuchMethodException {
        String name = field.getName();
        if (isBooleanType(field.getType())) {
            name = name.replaceAll("^is", "");
        }
        String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
        return clazz.getDeclaredMethod(methodName, field.getType());
    }

    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    private static boolean isBooleanType(Class<?> type) {
        return Boolean.class == type || boolean.class == type;
    }

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDefaultFamily() {
        return defaultFamily;
    }

    public boolean isAllField() {
        return allField;
    }

    public ColumnInfo getRowkeyColumn() {
        return rowkeyColumn;
    }

    public ColumnInfo getTimestampColumn() {
        return timestampColumn;
    }

    public ColumnInfo getVersionColumn() {
        return versionColumn;
    }

    public List<ColumnInfo> getColumnList() {
        return columnList;
    }

    public List<ColumnInfo> getDataColumns() {
        List<ColumnInfo> dataColumns = new ArrayList<>();
        for (ColumnInfo column : columnList) {
            if (!column.isRowkey() && !column.isTimestamp() && !column.isVersion()) {
                dataColumns.add(column);
            }
        }
        return dataColumns;
    }

    /**
     * 获取列族列表
     */
    public List<String> getColumnFamilies() {
        List<String> families = new ArrayList<>();
        for (ColumnInfo column : columnList) {
            if (column.getFamily() != null && !families.contains(column.getFamily())) {
                families.add(column.getFamily());
            }
        }
        return families;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
            "entityClass=" + entityClass.getName() +
            ", tableName='" + tableName + '\'' +
            ", defaultFamily='" + defaultFamily + '\'' +
            ", allField=" + allField +
            ", columns=" + columnList.size() +
            '}';
    }

}
