package com.king;

import com.king.sources.OracleSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: king
 * @Date: 2019-08-20
 * @Desc: TODO
 */

public class App {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new OracleSource()).print();

        try {
            env.execute("Flink add oracle source");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
