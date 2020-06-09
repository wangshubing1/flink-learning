package com.king.kudu.example;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.king.common.util.ExecutionEnvUtil;

import com.king.kudu.connector.KuduTableInfo;
import com.king.kudu.streaming.KuduSink;
import com.king.kudu.table.KuduTableSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.Collector;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public class KuduSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*StreamTableEnvironment sTableENV=StreamTableEnvironment.create(env);
        sTableENV.connect(
                new Kafka()
                        .version("0.11")
                        .topic("kafka_flink_kudu")
                        .startFromLatest()
                        .property("bootstrap.servers", "szsjhl-cdh-test-10-9-251-30.belle.lan:9092,szsjhl-cdh-test-10-9-251-31.belle.lan:9092,szsjhl-cdh-test-10-9-251-32.belle.lan:9092")
                        //.property("zookeeper.connect", "szsjhl-cdh-test-10-9-251-30.belle.lan:2181,szsjhl-cdh-test-10-9-251-31.belle.lan:2181,szsjhl-cdh-test-10-9-251-32.belle.lan:2181")
                        .property("group.id", "kafka_flink_kudu_group"))
                        .withFormat(
                                new Json()
                        );*/
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "szsjhl-cdh-test-10-9-251-30.belle.lan:9092,szsjhl-cdh-test-10-9-251-31.belle.lan:9092,szsjhl-cdh-test-10-9-251-32.belle.lan:9092");
        properties.setProperty("zookeeper.connect", "szsjhl-cdh-test-10-9-251-30.belle.lan:2181,szsjhl-cdh-test-10-9-251-31.belle.lan:2181,szsjhl-cdh-test-10-9-251-32.belle.lan:2181");
        properties.setProperty("group.id", "kafka_flink_kudu_group");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("kafka_flink_kudu", new SimpleStringSchema(), properties));
        //KuduTableInfo tableInfo =KuduTableInfo.
        //stream.print();
        SingleOutputStreamOperator<Map<String,Object>> mapSource = stream.map(s -> {
            Map<String, Object> map = new HashMap<String, Object>();
            JSONObject jsonObject = JSON.parseObject(JSON.parseObject(s).get("Data").toString());
            map.put("id", jsonObject.getInteger("id"));
            map.put("name", jsonObject.getString("name"));
            map.put("age", jsonObject.getInteger("age"));
            map.put("gender", jsonObject.getInteger("gender"));
            map.put("address", jsonObject.getString("address"));
            map.put("height", jsonObject.getDouble("height"));
            return map;
        });
        String kuduMaster="";
        String tableInfo="tablename";
        mapSource.addSink(new SinkKudu(kuduMaster,tableInfo));

        env.execute();

    }
}
