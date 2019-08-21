package com.king.model;

import java.util.Date;

/**
 * @Author: king
 * @Date: 2019-08-20
 * @Desc: TODO
 */

public class Test002 {
  public int id;
  public String text_name;
  public String user_name;
  public Date create_time;
  public Date update_time;

    public Test002(int id, String text_name, String user_name, java.sql.Date create_time, java.sql.Date update_time) {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText_name() {
        return text_name;
    }

    public void setText_name(String text_name) {
        this.text_name = text_name;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }




}
