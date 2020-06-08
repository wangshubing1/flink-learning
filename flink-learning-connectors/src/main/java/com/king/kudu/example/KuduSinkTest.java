package com.king.kudu.example;


import com.alibaba.fastjson.JSON;
import com.king.common.util.ExecutionEnvUtil;
import com.king.kudu.KuduSink;
import com.king.kudu.connector.KuduColumnInfo;
import com.king.kudu.connector.KuduTableInfo;
import com.king.kudu.serde.TestPojo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kudu.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public class KuduSinkTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.9.251.30:9092,10.9.251.31:9092,10.9.251.32:9092");
        props.setProperty("group.id", "king_test1");
       /* Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.9.251.30:9092,10.9.251.31:9092,10.9.251.32:9092");
        props.setProperty("group.id","king_test1");*/
        DataStream<String> data = env.addSource(
                new FlinkKafkaConsumer<String>(
                        "king_ogg_test5",
                        new SimpleStringSchema(),
                        props

                )
        );
        //System.out.println(data.toString());
        List<KuduColumnInfo> kuduColumnInfoList = new ArrayList<>();
        KuduColumnInfo columnInfo1 = KuduColumnInfo.Builder.createInteger("id").rangeKey(true).build();
        KuduColumnInfo columnInfo2 = KuduColumnInfo.Builder.createInteger("type").build();
        KuduColumnInfo columnInfo3 = KuduColumnInfo.Builder.createString("name").build();
        kuduColumnInfoList.add(columnInfo1);
        kuduColumnInfoList.add(columnInfo2);
        kuduColumnInfoList.add(columnInfo3);
        //KuduColumnInfo columnInfo1 = KuduColumnInfo.Builder
        KuduTableInfo kuduTableInfo = new KuduTableInfo.Builder("king_kudu")
                .addColumn(KuduColumnInfo.Builder.create("id", Type.INT32).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("type", Type.INT32).build())
                .addColumn(KuduColumnInfo.Builder.create("name", Type.STRING).build())
                .build();
        /*data.addSink(new KuduSink<MetricEvent>("szsjhl-cdh-test-10-9-251-32.belle.lan",
                kuduTableInfo,new PojoSerDe<>(MetricEvent.class)).withStrongConsistency());

        env.execute();*/
        data.addSink(new KuduSink<String>("10.9.251.32",
                kuduTableInfo,TestPojo.class).withStrongConsistency());

       /* .addSink(new KuduSink<>("szsjhl-cdh-test-10-9-251-32.belle.lan"
                ,kuduTableInfo,TestPojo.class));*/
        env.execute();

    }
}
