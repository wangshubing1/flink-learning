
package com.king.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public class RowOperationMapper extends AbstractSingleOperationMapper<Row> {

    protected RowOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    public RowOperationMapper(String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
    }

    @Override
    public Object getField(Row input, int i) {
        return input.getField(i);
    }
}
