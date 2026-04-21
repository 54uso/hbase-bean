package com.uso.hbase.query;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询条件构建器，封装 HBase Filter
 *
 * @author pengchuanjiang
 */
public class Criteria {

    private String family;

    private String qualifier;

    private CompareFilter.CompareOp compareOp;

    private Object value;

    private List<Filter> filters = new ArrayList<>();

    private FilterList.Operator operator = FilterList.Operator.MUST_PASS_ALL;

    private FilterType filterType = FilterType.COLUMN_VALUE;

    private enum FilterType {
        COLUMN_VALUE,
        ROW_KEY,
        VALUE,
        QUALIFIER,
        FAMILY,
        COLUMN_PREFIX
    }

    public Criteria() {
    }

    public Criteria(String family, String qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    /**
     * 创建列值条件构建器
     */
    public static Criteria where(String family, String qualifier) {
        return new Criteria(family, qualifier);
    }

    /**
     * 创建行键条件构建器
     */
    public static Criteria rowKey() {
        Criteria criteria = new Criteria();
        criteria.filterType = FilterType.ROW_KEY;
        return criteria;
    }

    /**
     * 创建值条件构建器
     */
    public static Criteria value(String family, String qualifier) {
        Criteria criteria = new Criteria(family, qualifier);
        criteria.filterType = FilterType.VALUE;
        return criteria;
    }

    /**
     * 创建列族条件构建器
     */
    public static Criteria family(String family) {
        Criteria criteria = new Criteria();
        criteria.family = family;
        criteria.filterType = FilterType.FAMILY;
        return criteria;
    }

    /**
     * 创建列限定符条件构建器
     */
    public static Criteria qualifier(String family, String qualifier) {
        Criteria criteria = new Criteria(family, qualifier);
        criteria.filterType = FilterType.QUALIFIER;
        return criteria;
    }

    /**
     * 创建列前缀条件构建器
     */
    public static Criteria columnPrefix(String family, String prefix) {
        Criteria criteria = new Criteria(family, null);
        criteria.value = prefix;
        criteria.filterType = FilterType.COLUMN_PREFIX;
        return criteria;
    }

    /**
     * 等于
     */
    public Criteria equalTo(Object value) {
        this.compareOp = CompareFilter.CompareOp.EQUAL;
        this.value = value;
        return this;
    }

    /**
     * 不等于
     */
    public Criteria notEqualTo(Object value) {
        this.compareOp = CompareFilter.CompareOp.NOT_EQUAL;
        this.value = value;
        return this;
    }

    /**
     * 大于
     */
    public Criteria greaterThan(Object value) {
        this.compareOp = CompareFilter.CompareOp.GREATER;
        this.value = value;
        return this;
    }

    /**
     * 大于等于
     */
    public Criteria greaterThanOrEqualTo(Object value) {
        this.compareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
        this.value = value;
        return this;
    }

    /**
     * 小于
     */
    public Criteria lessThan(Object value) {
        this.compareOp = CompareFilter.CompareOp.LESS;
        this.value = value;
        return this;
    }

    /**
     * 小于等于
     */
    public Criteria lessThanOrEqualTo(Object value) {
        this.compareOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
        this.value = value;
        return this;
    }

    /**
     * 包含子串
     */
    public Criteria contains(String value) {
        this.compareOp = CompareFilter.CompareOp.EQUAL;
        this.value = new SubstringComparator(value);
        return this;
    }

    /**
     * 正则匹配
     */
    public Criteria matchesRegex(String regex) {
        this.compareOp = CompareFilter.CompareOp.EQUAL;
        this.value = new RegexStringComparator(regex);
        return this;
    }

    /**
     * AND 条件
     */
    public Criteria and(Criteria other) {
        this.operator = FilterList.Operator.MUST_PASS_ALL;
        if (other != null) {
            this.filters.add(other.toFilter());
        }
        return this;
    }

    /**
     * OR 条件
     */
    public Criteria or(Criteria other) {
        this.operator = FilterList.Operator.MUST_PASS_ONE;
        if (other != null) {
            this.filters.add(other.toFilter());
        }
        return this;
    }

    /**
     * 转换为 HBase Filter
     */
    public Filter toFilter() {
        Filter mainFilter = createMainFilter();

        if (this.filters.isEmpty()) {
            return mainFilter;
        }

        FilterList filterList = new FilterList(this.operator);
        if (mainFilter != null) {
            filterList.addFilter(mainFilter);
        }
        filterList.addFilter(this.filters);
        return filterList;
    }

    private Filter createMainFilter() {
        if (this.compareOp == null) {
            return null;
        }

        switch (this.filterType) {
            case ROW_KEY:
                return createRowFilter();
            case COLUMN_VALUE:
                return createColumnValueFilter();
            case VALUE:
                return createValueFilter();
            case QUALIFIER:
                return createQualifierFilter();
            case FAMILY:
                return createFamilyFilter();
            case COLUMN_PREFIX:
                return createColumnPrefixFilter();
            default:
                return createColumnValueFilter();
        }
    }

    private Filter createRowFilter() {
        return new RowFilter(this.compareOp, createComparator());
    }

    private Filter createColumnValueFilter() {
        if (this.family == null || this.qualifier == null) {
            return null;
        }
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            Bytes.toBytes(this.family),
            Bytes.toBytes(this.qualifier),
            this.compareOp,
            toBytes(this.value)
        );
        filter.setFilterIfMissing(true);
        return filter;
    }

    private Filter createValueFilter() {
        if (this.family == null || this.qualifier == null) {
            return null;
        }
        return new ValueFilter(this.compareOp, createComparator());
    }

    private Filter createQualifierFilter() {
        return new QualifierFilter(this.compareOp, createComparator());
    }

    private Filter createFamilyFilter() {
        return new FamilyFilter(this.compareOp, createComparator());
    }

    private Filter createColumnPrefixFilter() {
        if (this.value instanceof String) {
            return new ColumnPrefixFilter(Bytes.toBytes((String) this.value));
        }
        if (this.value instanceof byte[]) {
            return new ColumnPrefixFilter((byte[]) this.value);
        }
        return null;
    }

    private org.apache.hadoop.hbase.filter.ByteArrayComparable createComparator() {
        if (this.value == null) {
            return new BinaryComparator(new byte[0]);
        }
        if (this.value instanceof org.apache.hadoop.hbase.filter.ByteArrayComparable) {
            return (org.apache.hadoop.hbase.filter.ByteArrayComparable) this.value;
        }
        return new BinaryComparator(toBytes(this.value));
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return Bytes.toBytes((String) value);
        }
        if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        }
        if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        }
        if (value instanceof Double) {
            return Bytes.toBytes((Double) value);
        }
        if (value instanceof Float) {
            return Bytes.toBytes((Float) value);
        }
        if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        }
        if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        }
        return Bytes.toBytes(value.toString());
    }

    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public CompareFilter.CompareOp getCompareOp() {
        return compareOp;
    }

    public Object getValue() {
        return value;
    }

}
