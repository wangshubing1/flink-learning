package com.king.Elasticsearch;

import com.king.Elasticsearch.sink.ElasticSearchSinkUtil;
import com.king.common.model.MetricEvent;
import com.king.common.util.ExecutionEnvUtil;
import com.king.common.util.GsonUtil;
import com.king.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.apache.kudu.shaded.org.checkerframework.checker.units.qual.K;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

import static com.king.common.constant.Constants.*;

/**
 * @Author: king
 * @Date: 2019-08-30
 * @Desc: TODO
 */

public class Demo {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env =ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize =parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS,40);
        int sinkParallelism =parameterTool.getInt(STREAM_SINK_PARALLELISM,5);

        ElasticSearchSinkUtil.addSink(esAddresses,bulkSize,sinkParallelism,data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(KING + "_" + metric.getName())
                            .type(KING)
                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
                });

        env.execute("flink es");
    }
}
