package com.uso.hbase.metadata;

/**
 * 列族信息
 *
 * @author pengchuanjiang
 */
public class ColumnFamilyInfo {

    private String family;

    private byte[] familyBytes;

    public ColumnFamilyInfo(String family) {
        this.family = family;
        this.familyBytes = family.getBytes();
    }

    public String getFamily() {
        return family;
    }

    public byte[] getFamilyBytes() {
        return familyBytes;
    }

}
