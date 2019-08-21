package com.king.sources;

import com.king.model.Test002;
import com.king.utils.OracleUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: king
 * @Date: 2019-08-20
 * @Desc: TODO
 */

public class OracleSource extends RichSourceFunction<Test002> {
    PreparedStatement ps;
    private Connection connection;
    String driver = "oracle.jdbc.driver.OracleDriver";
    String url = "jdbc:oracle:thin:@//172.17.210.184:1521/hdwms";
    String userName = "tcloud";
    String password = "tcloud";

    @Override
    public void open(Configuration conf) throws Exception {
        super.open(conf);
        connection = OracleUtil.getConnection(driver, url, userName, password);
        String sql = "select * from TCLOUD.TEST02";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Test002> ctx) throws Exception {
        ResultSet res = ps.executeQuery();
        while (res.next()) {
            Test002 test002 = new Test002(
                    res.getInt("id"),
                    res.getString("text_name"),
                    res.getString("user_name"),
                    res.getDate("create_time"),
                    res.getDate("update_time"));

            ctx.collect(test002);
        }

    }

    @Override
    public void cancel() {

    }
}
