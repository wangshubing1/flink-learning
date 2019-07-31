package com.king.common.util;

/**
 * @Author: king
 * @Date: 2019-07-31
 * @Desc: TODO
 */

public class FileUtils {
    /**
     * 获取当前项目的路径
     * @return
     */
    public static String getFilePath(){
        String path = System.getProperty("user.dir");
        return path;
    }

    public static void main(String[] args) {
        System.out.println(getFilePath());
    }

}
