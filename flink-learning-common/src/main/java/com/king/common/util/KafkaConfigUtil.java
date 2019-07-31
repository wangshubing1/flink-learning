package com.king.common.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.king.common.constant.Constants;
import com.king.common.model.MetricEvent;
import com.king.common.schema.MetricSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


/**
 * @Author: king
 * @Date: 2019-07-31
 * @Desc: TODO
 */

public class KafkaConfigUtil {
    public static Properties buildKafkaProps(){
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    /**
     * kafka 参数配置
     * @param parameterTool
     * @return
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool){
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(Constants.KAFKA_BROKERS, Constants.DEFAULT_KAFKA_BROKERS));
        props.put("zookeeper.connect", parameterTool.get(Constants.KAFKA_ZOOKEEPER_CONNECT, Constants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put("group.id", parameterTool.get(Constants.KAFKA_GROUP_ID, Constants.DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", parameterTool.get(Constants.KAFKA_OFFSET,Constants.DEFAULT_KAFKA_OFFSET));
        return props;
    }
    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) throws IllegalAccessException {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameter.getRequired(Constants.METRICS_TOPIC);
        Long time = parameter.getLong(Constants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time);
    }
    /**
     * @param env
     * @param topic
     * @param time  订阅的时间
     * @return
     * @throws IllegalAccessException
     */
    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env, String topic, Long time) throws IllegalAccessException {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(
                topic,
                new MetricSchema(),
                props);
        //重置offset到time时刻
        if (time != 0L) {
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
        props.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(Constants.METRICS_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));

        consumer.close();
        return partitionOffset;
    }

    /*public static SingleOutputStreamOperator<MetricEvent> parseSource(DataStreamSource<MetricEvent> dataStreamSource) {
        return dataStreamSource.assignTimestampsAndWatermarks(new MetricWatermark());
    }*/
}
