package com.king.kudu.serde;

/**
 * @Author: king
 * @Date: 2019-08-29
 * @Desc: TODO
 */

public class TestPojo {
    private Integer id;
    private Integer type;
    private String name;
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "TestPojo{" +
                "id=" + id +
                ", name='" + name +
                ", type=" +type +'\''+
                '}';
    }
}
