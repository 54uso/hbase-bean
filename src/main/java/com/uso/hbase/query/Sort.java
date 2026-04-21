package com.uso.hbase.query;

/**
 * 排序条件
 *
 * @author pengchuanjiang
 */
public class Sort {

    private String family;

    private String qualifier;

    private Order order;

    public Sort(String family, String qualifier, Order order) {
        this.family = family;
        this.qualifier = qualifier;
        this.order = order;
    }

    public static Sort asc(String family, String qualifier) {
        return new Sort(family, qualifier, Order.ASC);
    }

    public static Sort desc(String family, String qualifier) {
        return new Sort(family, qualifier, Order.DESC);
    }

    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public Order getOrder() {
        return order;
    }

}
