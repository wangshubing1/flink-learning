
package com.king.kudu.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@Internal
public class KuduReaderIterator {

    private KuduScanner scanner;
    private RowResultIterator rowIterator;

    public KuduReaderIterator(KuduScanner scanner) throws KuduException {
        this.scanner = scanner;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public Row next() {
        RowResult row = this.rowIterator.next();
        return toFlinkRow(row);
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }

    private Row toFlinkRow(RowResult row) {
        Schema schema = row.getColumnProjection();

        Row values = new Row(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            values.setField(pos, row.getObject(name));
        });
        return values;
    }
}
