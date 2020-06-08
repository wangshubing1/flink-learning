package examples.kafka;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kudu.shaded.org.checkerframework.checker.units.qual.K;

import java.util.*;

import static examples.kafka.Time2Date.timeToDate;

/**
 * @Author: king
 * @Date: 2019-09-09
 * @Desc: TODO
 */

public class Kafka2kudu {
    public static void main(String[] args) throws Exception {
        //final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.9.251.30:9092,10.9.251.31:9092,10.9.251.32:9092");
        properties.setProperty("group.id", "king_test1");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("gyl_ept_oplog_kudu02", new SimpleStringSchema(), properties));
        stream.print();
        Map<String,Object> col =new HashMap<>();
        stream.filter(value->{
            JSONObject jsonObject = JSONObject.parseObject(value);
            String opType = jsonObject.getString("op");
            String[] dbTableName = jsonObject.getString("ns").split(".");
            String tableName = dbTableName[1];
            KuduBean kuduBean =new KuduBean();
            JSONObject jsonData = JSONObject.parseObject(jsonObject.getString("o"));
            Map<String,Object> jsonDataMap = new HashMap<>();
            Utils.json2Map(jsonDataMap,jsonObject.getString("o"));
            //Map<String,Object> jsonDataMap = JSONObject.parseObject(jsonObject.getString("o"));
            List<String> jsonList =new ArrayList();
            if (tableName.equals("bill_way_express_trace")){
                if(opType.equals("i")||opType.equals("u")){
                    for (String key:jsonDataMap.keySet()){
                        jsonList.add(key);
                        List<String> newList=Utils.listDifferenceSet(kuduBean.getColList(),jsonList);
                        for (String list:newList){

                        }
                    }
                    String create_time = timeToDate(jsonData.getLong("create_time").toString());
                    String opt_time = timeToDate(jsonData.getLong("opt_time").toString());
                    String update_time = timeToDate(jsonData.getLong("update_time").toString());
                    String ods_update_time = timeToDate(String.valueOf(System.currentTimeMillis()));
                    col.put("_id",jsonData.getString("_id"));
                    col.put("sharding_yyyymm",create_time.substring(0,7));
                    col.put("express_company",jsonData.getString("express_company"));
                    col.put("express_no",jsonData.getString("express_no"));
                    col.put("opt_time",opt_time);
                    col.put("opt_type",jsonData.getString("opt_type"));
                    col.put("company_id",jsonData.getString("company_id"));
                    col.put("opt_desc",jsonData.getString("opt_desc"));
                    col.put("opt_user",jsonData.getString("opt_user"));
                    col.put("opt_tel",jsonData.getString("opt_tel"));
                    col.put("courier",jsonData.getString("courier"));
                    col.put("courier_tel",jsonData.getString("courier_tel"));
                    col.put("data_zone",jsonData.getString("data_zone"));
                    col.put("create_time",create_time);
                    col.put("update_time",update_time);
                    col.put("del_tag",jsonData.getString("del_tag"));
                    col.put("ods_update_time",ods_update_time);

                }
            }

            return true;
        });
        env.execute("Kafka2kudu");
    }

}
