package com.uso.hbase.page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 分页结果
 *
 * @author pengchuanjiang
 */
public class Page<T> implements Iterable<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private List<T> content;

    private Pageable pageable;

    private long total;

    public Page() {
        this(new ArrayList<>(), Pageable.of(1, 10), 0);
    }

    public Page(List<T> content, Pageable pageable, long total) {
        this.content = content != null ? content : new ArrayList<>();
        this.pageable = pageable;
        this.total = total;
    }

    /**
     * 创建分页结果
     */
    public static <T> Page<T> of(List<T> content, Pageable pageable, long total) {
        return new Page<>(content, pageable, total);
    }

    /**
     * 创建空分页结果
     */
    public static <T> Page<T> empty() {
        return empty(Pageable.of(1, 10));
    }

    /**
     * 创建空分页结果
     */
    public static <T> Page<T> empty(Pageable pageable) {
        return new Page<>(new ArrayList<>(), pageable, 0);
    }

    /**
     * 获取总页数
     */
    public int getTotalPages() {
        if (pageable.getPageSize() == 0) {
            return 0;
        }
        return (int) Math.ceil((double) total / (double) pageable.getPageSize());
    }

    /**
     * 获取总记录数
     */
    public long getTotalElements() {
        return total;
    }

    /**
     * 获取当前页码
     */
    public int getPageNum() {
        return pageable.getPageNum();
    }

    /**
     * 获取每页大小
     */
    public int getPageSize() {
        return pageable.getPageSize();
    }

    /**
     * 获取内容列表
     */
    public List<T> getContent() {
        return Collections.unmodifiableList(content);
    }

    /**
     * 获取分页参数
     */
    public Pageable getPageable() {
        return pageable;
    }

    /**
     * 是否有内容
     */
    public boolean hasContent() {
        return !content.isEmpty();
    }

    /**
     * 是否是第一页
     */
    public boolean isFirst() {
        return getPageNum() == 1;
    }

    /**
     * 是否是最后一页
     */
    public boolean isLast() {
        return getPageNum() >= getTotalPages();
    }

    /**
     * 是否有下一页
     */
    public boolean hasNext() {
        return !isLast();
    }

    /**
     * 是否有上一页
     */
    public boolean hasPrevious() {
        return !isFirst();
    }

    /**
     * 获取下一页的分页参数
     */
    public Pageable nextPageable() {
        return Pageable.of(getPageNum() + 1, getPageSize());
    }

    /**
     * 获取上一页的分页参数
     */
    public Pageable previousPageable() {
        return Pageable.of(Math.max(1, getPageNum() - 1), getPageSize());
    }

    /**
     * 获取内容大小
     */
    public int getNumberOfElements() {
        return content.size();
    }

    /**
     * 获取流
     */
    public Stream<T> stream() {
        return content.stream();
    }

    /**
     * 是否为空
     */
    public boolean isEmpty() {
        return content.isEmpty();
    }

    /**
     * 获取第一个元素
     */
    public T getFirst() {
        if (content.isEmpty()) {
            throw new java.util.NoSuchElementException("Page is empty");
        }
        return content.get(0);
    }

    /**
     * 转换内容类型
     */
    public <U> Page<U> map(Function<? super T, ? extends U> converter) {
        List<U> convertedContent = content.stream()
            .map(converter)
            .collect(Collectors.toList());
        return new Page<>(convertedContent, pageable, total);
    }

    @Override
    public Iterator<T> iterator() {
        return content.iterator();
    }

    /**
     * 获取可迭代的分页内容
     */
    public Iterable<T> get() {
        return content;
    }

    @Override
    public String toString() {
        return "Page{" +
            "content size=" + content.size() +
            ", pageNum=" + getPageNum() +
            ", pageSize=" + getPageSize() +
            ", total=" + total +
            ", totalPages=" + getTotalPages() +
            '}';
    }

    public void setContent(List<T> content) {
        this.content = content;
    }

    public void setPageable(Pageable pageable) {
        this.pageable = pageable;
    }

    public void setTotal(long total) {
        this.total = total;
    }

}
