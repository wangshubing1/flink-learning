package com.king.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: king
 * @Date: 2020-06-17
 * @Desc: TODO
 */

public class KafkaSourceUtil {
    public static DataStream<String> getKafkaSource(StreamExecutionEnvironment env, String topicName){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "szsjhl-cdh-test-10-9-251-30.belle.lan:9092,szsjhl-cdh-test-10-9-251-31.belle.lan:9092,szsjhl-cdh-test-10-9-251-32.belle.lan:9092");
        properties.setProperty("zookeeper.connect", "szsjhl-cdh-test-10-9-251-30.belle.lan:2181,szsjhl-cdh-test-10-9-251-31.belle.lan:2181,szsjhl-cdh-test-10-9-251-32.belle.lan:2181");
        properties.setProperty("group.id", "kafka_flink_kudu_group");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), properties));
        return stream;
    }
}
