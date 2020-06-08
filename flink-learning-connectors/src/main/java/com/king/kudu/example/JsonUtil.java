package com.king.kudu.example;


import com.alibaba.fastjson.JSONObject;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */

public class JsonUtil {

    public static String testJson;

    public static JSONObject testJson(Object json){
        JSONObject jsonObject =JSONObject.parseObject(json.toString());
        JSONObject dataJson =jsonObject.getJSONObject("Data");
        return dataJson;
    }
}
