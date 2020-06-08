
package com.king.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public class TupleOperationMapper<T extends Tuple> extends AbstractSingleOperationMapper<T> {

    protected TupleOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    public TupleOperationMapper(String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
    }

    @Override
    public Object getField(T input, int i) {
        return input.getField(i);
    }
}
