package examples.kafka;

/**
 * @Author: king
 * @Date: Create in 2021/4/15
 * @Desc: TODO
 */
import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
public class DbUtils {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://10.9.251.30:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("belle@019");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }
}
