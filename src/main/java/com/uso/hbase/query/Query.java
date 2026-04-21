package com.uso.hbase.query;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * HBase 查询构建器
 *
 * @author pengchuanjiang
 */
public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    private byte[] startRow;

    private byte[] stopRow;

    private boolean includeStartRow = true;

    private boolean includeStopRow = false;

    private int maxVersions = 1;

    private int limit = -1;

    private int offset = 0;

    private List<Criteria> criteriaList = new ArrayList<>();

    private List<Sort> sorts = new ArrayList<>();

    private List<String> families = new ArrayList<>();

    private List<String> qualifiers = new ArrayList<>();

    public Query() {
    }

    /**
     * 创建查询构建器
     */
    public static Query create() {
        return new Query();
    }

    /**
     * 设置起始行
     */
    public Query startRow(byte[] startRow) {
        return startRow(startRow, true);
    }

    /**
     * 设置起始行
     */
    public Query startRow(byte[] startRow, boolean inclusive) {
        this.startRow = startRow;
        this.includeStartRow = inclusive;
        return this;
    }

    /**
     * 设置结束行
     */
    public Query stopRow(byte[] stopRow) {
        return stopRow(stopRow, false);
    }

    /**
     * 设置结束行
     */
    public Query stopRow(byte[] stopRow, boolean inclusive) {
        this.stopRow = stopRow;
        this.includeStopRow = inclusive;
        return this;
    }

    /**
     * 设置前缀
     */
    public Query prefix(byte[] prefix) {
        if (prefix != null && prefix.length > 0) {
            this.startRow = prefix;
            this.includeStartRow = true;
            byte[] stopRow = new byte[prefix.length];
            System.arraycopy(prefix, 0, stopRow, 0, prefix.length);
            int i = stopRow.length - 1;
            while (i >= 0) {
                stopRow[i]++;
                if (stopRow[i] != 0) {
                    break;
                }
                i--;
            }
            this.stopRow = stopRow;
            this.includeStopRow = false;
        }
        return this;
    }

    /**
     * 设置行键范围
     */
    public Query rowKeyRange(byte[] startRow, byte[] stopRow) {
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.includeStartRow = true;
        this.includeStopRow = false;
        return this;
    }

    /**
     * 添加行键范围条件
     */
    public Query withRowKey(String family, String qualifier, Object value) {
        this.criteriaList.add(Criteria.where(family, qualifier).equalTo(value));
        return this;
    }

    /**
     * 添加条件
     */
    public Query addCriteria(Criteria criteria) {
        if (criteria != null) {
            this.criteriaList.add(criteria);
        }
        return this;
    }

    /**
     * 添加排序
     */
    public Query orderBy(String family, String qualifier, Order order) {
        this.sorts.add(new Sort(family, qualifier, order));
        return this;
    }

    /**
     * 添加升序排序
     */
    public Query orderByAsc(String family, String qualifier) {
        return orderBy(family, qualifier, Order.ASC);
    }

    /**
     * 添加降序排序
     */
    public Query orderByDesc(String family, String qualifier) {
        return orderBy(family, qualifier, Order.DESC);
    }

    /**
     * 限制返回行数
     */
    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 跳过行数
     */
    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }

    /**
     * 分页
     */
    public Query page(int pageNum, int pageSize) {
        this.offset = (pageNum - 1) * pageSize;
        this.limit = pageSize;
        return this;
    }

    /**
     * 设置最大版本数
     */
    public Query maxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
        return this;
    }

    /**
     * 返回所有版本
     */
    public Query allVersions() {
        this.maxVersions = Integer.MAX_VALUE;
        return this;
    }

    /**
     * 添加列族
     */
    public Query addFamily(String family) {
        if (family != null && !family.isEmpty()) {
            this.families.add(family);
        }
        return this;
    }

    /**
     * 添加多个列族
     */
    public Query addFamilies(String... families) {
        if (families != null) {
            this.families.addAll(Arrays.asList(families));
        }
        return this;
    }

    /**
     * 添加列限定符
     */
    public Query addColumn(String family, String qualifier) {
        if (family != null && qualifier != null) {
            this.qualifiers.add(family + ":" + qualifier);
        }
        return this;
    }

    /**
     * 添加多个列限定符
     */
    public Query addColumns(String family, String... qualifiers) {
        if (family != null && qualifiers != null) {
            for (String qualifier : qualifiers) {
                this.qualifiers.add(family + ":" + qualifier);
            }
        }
        return this;
    }

    /**
     * 转换为 Filter
     */
    public Filter toFilter() {
        if (this.criteriaList.isEmpty()) {
            return null;
        }

        if (this.criteriaList.size() == 1) {
            return this.criteriaList.get(0).toFilter();
        }

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        for (Criteria criteria : this.criteriaList) {
            Filter filter = criteria.toFilter();
            if (filter != null) {
                filterList.addFilter(filter);
            }
        }
        return filterList;
    }

    /**
     * 是否有行键范围
     */
    public boolean hasRowKeyRange() {
        return this.startRow != null || this.stopRow != null;
    }

    /**
     * 是否有过滤条件
     */
    public boolean hasFilter() {
        return !this.criteriaList.isEmpty();
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getStopRow() {
        return stopRow;
    }

    public boolean isIncludeStartRow() {
        return includeStartRow;
    }

    public boolean isIncludeStopRow() {
        return includeStopRow;
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public List<Criteria> getCriteriaList() {
        return criteriaList;
    }

    public List<Sort> getSorts() {
        return sorts;
    }

    public List<String> getFamilies() {
        return families;
    }

    public List<String> getQualifiers() {
        return qualifiers;
    }

    @Override
    public String toString() {
        return "Query{" +
            "hasStartRow=" + (startRow != null) +
            ", hasStopRow=" + (stopRow != null) +
            ", maxVersions=" + maxVersions +
            ", limit=" + limit +
            ", offset=" + offset +
            ", criteriaCount=" + criteriaList.size() +
            ", families=" + families +
            '}';
    }

}
