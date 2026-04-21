package com.uso.hbase.example;

import com.uso.hbase.annotation.*;

/**
 * 订单实体类示例
 *
 * @author pengchuanjiang
 */
@HTable(value = "order", defaultFamily = "data")
public class Order {

    @HRowkey
    private String orderId;

    @HColumn(qualifier = "userId")
    private String userId;

    @HColumn(qualifier = "amount")
    private Double amount;

    @HColumn(qualifier = "status")
    private String status;

    @HColumn(qualifier = "createTime")
    private Long createTime;

    @HColumn(qualifier = "updateTime")
    private Long updateTime;

    @HColumn(family = "extra", qualifier = "remark")
    private String remark;

    @HTimestamp
    private Long timestamp;

    public Order() {
    }

    public Order(String orderId, String userId, Double amount, String status) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
        this.createTime = System.currentTimeMillis();
        this.updateTime = this.createTime;
    }

    public Order(String orderId, String userId, Double amount, String status, String remark) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
        this.remark = remark;
        this.createTime = System.currentTimeMillis();
        this.updateTime = this.createTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" +
            "orderId='" + orderId + '\'' +
            ", userId='" + userId + '\'' +
            ", amount=" + amount +
            ", status='" + status + '\'' +
            ", createTime=" + createTime +
            ", updateTime=" + updateTime +
            ", remark='" + remark + '\'' +
            '}';
    }

}
