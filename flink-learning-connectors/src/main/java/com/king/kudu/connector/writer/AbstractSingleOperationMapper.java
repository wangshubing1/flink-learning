
package com.king.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public abstract class AbstractSingleOperationMapper<T> implements KuduOperationMapper<T> {

    protected final String[] columnNames;
    private final KuduOperation operation;

    protected AbstractSingleOperationMapper(String[] columnNames) {
        this(columnNames, null);
    }

    public AbstractSingleOperationMapper(String[] columnNames, KuduOperation operation) {
        this.columnNames = columnNames;
        this.operation = operation;
    }

    /**
     * Returns the object corresponding to the given column index.
     *
     * @param input Input element
     * @param i     Column index
     * @return Column value
     */
    public abstract Object getField(T input, int i);

    public Optional<Operation> createBaseOperation(T input, KuduTable table) {
        if (operation == null) {
            throw new UnsupportedOperationException("createBaseOperation must be overridden if no operation specified in constructor");
        }
        switch (operation) {
            case INSERT:
                return Optional.of(table.newInsert());
            case UPDATE:
                return Optional.of(table.newUpdate());
            case UPSERT:
                return Optional.of(table.newUpsert());
            case DELETE:
                return Optional.of(table.newDelete());
            default:
                throw new RuntimeException("Unknown operation " + operation);
        }
    }

    @Override
    public List<Operation> createOperations(T input, KuduTable table) {
        Optional<Operation> operationOpt = createBaseOperation(input, table);
        if (!operationOpt.isPresent()) {
            return Collections.emptyList();
        }

        Operation operation = operationOpt.get();
        PartialRow partialRow = operation.getRow();

        for (int i = 0; i < columnNames.length; i++) {
            partialRow.addObject(columnNames[i], getField(input, i));
        }

        return Collections.singletonList(operation);
    }

    public enum KuduOperation {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }
}
