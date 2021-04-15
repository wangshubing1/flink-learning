package examples.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @Author: king
 * @Date: Create in 2021/4/15
 * @Desc: TODO
 */
public class Kafka2Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "szsjhl-cdh-test-10-9-251-30.belle.lan:9092,szsjhl-cdh-test-10-9-251-31.belle.lan:9092,szsjhl-cdh-test-10-9-251-32.belle.lan:9092");
        properties.setProperty("group.id", "bdc_yg_outside_for_bellespider_group_01");
//        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.connect(new Kafka()
//                .version("1.0.1")
//                .topic("bdc_yg_outside_for_bellespider")
//                .properties(properties))
//                .withFormat()
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("bdc_yg_outside_for_bellespider", new SimpleStringSchema(), properties);
        //指定消费位置
        //myConsumer.setStartFromTimestamp(1618365876000L);
        DataStream<String> stream = env.addSource(myConsumer);
        SingleOutputStreamOperator<tbl_order_seller> seller_stream =
                stream.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        JsonParser parser = new JsonParser();
                        JsonObject value = (JsonObject) parser.parse(s);
                        String table_name = value.get("Table").getAsString();
                        return table_name.equalsIgnoreCase("tbl_order_seller_source");
                    }
                }).map(new MapFunction<String, tbl_order_seller>() {
                    @Override
                    public tbl_order_seller map(String s) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(s, tbl_order_seller.class);
                    }
                });
        seller_stream.print();
        seller_stream.addSink(new MysqlSink());
        env.execute("数据同步");
    }
}
