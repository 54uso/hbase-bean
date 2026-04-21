package com.uso.hbase.metadata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * 列信息
 *
 * @author pengchuanjiang
 */
public class ColumnInfo {

    private String fieldName;

    private String family;

    private String qualifier;

    private byte[] familyBytes;

    private byte[] qualifierBytes;

    private Class<?> fieldType;

    private Field field;

    private Method getMethod;

    private Method setMethod;

    private boolean isRowkey;

    private boolean isTimestamp;

    private boolean isVersion;

    public ColumnInfo() {
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
        this.familyBytes = family != null ? family.getBytes() : null;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
        this.qualifierBytes = qualifier != null ? qualifier.getBytes() : null;
    }

    public byte[] getFamilyBytes() {
        return familyBytes;
    }

    public byte[] getQualifierBytes() {
        return qualifierBytes;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Method getGetMethod() {
        return getMethod;
    }

    public void setGetMethod(Method getMethod) {
        this.getMethod = getMethod;
    }

    public Method getSetMethod() {
        return setMethod;
    }

    public void setSetMethod(Method setMethod) {
        this.setMethod = setMethod;
    }

    public boolean isRowkey() {
        return isRowkey;
    }

    public void setRowkey(boolean rowkey) {
        isRowkey = rowkey;
    }

    public boolean isTimestamp() {
        return isTimestamp;
    }

    public void setTimestamp(boolean timestamp) {
        isTimestamp = timestamp;
    }

    public boolean isVersion() {
        return isVersion;
    }

    public void setVersion(boolean version) {
        isVersion = version;
    }

}
