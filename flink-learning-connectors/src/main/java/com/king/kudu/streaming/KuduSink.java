
package com.king.kudu.streaming;

import com.king.kudu.connector.KuduTableInfo;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import com.king.kudu.connector.failure.DefaultKuduFailureHandler;
import com.king.kudu.connector.failure.KuduFailureHandler;
import com.king.kudu.connector.writer.KuduOperationMapper;
import com.king.kudu.connector.writer.KuduWriter;
import com.king.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;


@PublicEvolving
public class KuduSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<IN> opsMapper;
    private transient KuduWriter kuduWriter;

    /**
     * 对指定的kudu表进行操作
     * for the incoming stream elements.
     *
     * @param writerConfig 写入配置
     * @param tableInfo    目标表信息
     * @param opsMapper    输入kudu的映射逻辑
     */
    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    /**
     * 对指定的kudu表进行操作
     *
     * @param writerConfig   写入配置
     * @param tableInfo      目标表信息
     * @param opsMapper      输入kudu的映射逻辑
     * @param failureHandler 自定义故障处理实例
     */
    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }

    @Override
    public void invoke(IN value) throws Exception {
        try {
            kuduWriter.write(value);
        } catch (ClassCastException e) {
            failureHandler.onTypeMismatch(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (kuduWriter != null) {
            kuduWriter.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        kuduWriter.flushAndCheckErrors();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

}
