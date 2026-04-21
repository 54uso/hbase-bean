package com.uso.hbase.page;

import java.io.Serializable;

/**
 * 分页请求参数
 *
 * @author pengchuanjiang
 */
public class Pageable implements Serializable {

    private static final long serialVersionUID = 1L;

    private int pageNum;

    private int pageSize;

    public Pageable() {
        this(1, 10);
    }

    public Pageable(int pageNum, int pageSize) {
        this.pageNum = Math.max(1, pageNum);
        this.pageSize = Math.max(1, Math.min(pageSize, 1000));
    }

    /**
     * 创建分页请求
     */
    public static Pageable of(int pageNum, int pageSize) {
        return new Pageable(pageNum, pageSize);
    }

    /**
     * 创建第一页
     */
    public static Pageable first(int pageSize) {
        return new Pageable(1, pageSize);
    }

    /**
     * 获取偏移量
     */
    public int getOffset() {
        return (pageNum - 1) * pageSize;
    }

    /**
     * 获取下一页
     */
    public Pageable next() {
        return Pageable.of(pageNum + 1, pageSize);
    }

    /**
     * 获取上一页
     */
    public Pageable previous() {
        return Pageable.of(Math.max(1, pageNum - 1), pageSize);
    }

    /**
     * 获取第一页
     */
    public Pageable first() {
        return Pageable.of(1, pageSize);
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = Math.max(1, pageNum);
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = Math.max(1, Math.min(pageSize, 1000));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pageable pageable = (Pageable) o;
        return pageNum == pageable.pageNum && pageSize == pageable.pageSize;
    }

    @Override
    public int hashCode() {
        return 31 * pageNum + pageSize;
    }

    @Override
    public String toString() {
        return "Pageable{" +
            "pageNum=" + pageNum +
            ", pageSize=" + pageSize +
            '}';
    }

}
