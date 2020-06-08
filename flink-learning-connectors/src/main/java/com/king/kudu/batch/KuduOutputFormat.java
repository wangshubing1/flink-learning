
package com.king.kudu.batch;

import com.king.kudu.connector.KuduTableInfo;
import com.king.kudu.connector.failure.*;
import com.king.kudu.connector.writer.*;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public class KuduOutputFormat<IN> extends RichOutputFormat<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<IN> opsMapper;

    private transient KuduWriter kuduWriter;

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }

    @Override
    public void writeRecord(IN row) throws IOException {
        kuduWriter.write(row);
    }

    @Override
    public void close() throws IOException {
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
