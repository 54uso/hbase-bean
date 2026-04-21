package com.uso.hbase.example;

import com.uso.hbase.annotation.*;

/**
 * 用户实体类示例
 *
 * @author pengchuanjiang
 */
@HTable(value = "user", defaultFamily = "info")
public class User {

    @HRowkey
    private String id;

    @HColumn(qualifier = "name")
    private String name;

    @HColumn(qualifier = "age")
    private Integer age;

    @HColumn(qualifier = "email")
    private String email;

    @HColumn(family = "detail", qualifier = "address")
    private String address;

    @HColumn(qualifier = "status")
    private String status;

    @HTimestamp
    private Long timestamp;

    public User() {
    }

    public User(String id, String name, Integer age, String email) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.email = email;
    }

    public User(String id, String name, Integer age, String email, String address) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.email = email;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "User{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", age=" + age +
            ", email='" + email + '\'' +
            ", address='" + address + '\'' +
            ", status='" + status + '\'' +
            ", timestamp=" + timestamp +
            '}';
    }

}
