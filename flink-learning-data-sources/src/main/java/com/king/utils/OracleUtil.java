package com.king.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: king
 * @Date: 2019-08-20
 * @Desc: TODO
 */

public class OracleUtil {
    public static Connection getConnection(String driver,String url,String user,String password){
        Connection con =null;
        try{
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        }catch (Exception e){
            System.out.println("---------Oracle get connection has exception ,msg = "+ e.getMessage());
        }
        return con;
    }
    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    public void close(Connection connection, PreparedStatement ps) throws Exception {
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
