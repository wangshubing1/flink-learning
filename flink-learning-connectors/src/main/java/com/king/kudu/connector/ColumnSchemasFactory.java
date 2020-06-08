
package com.king.kudu.connector;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.ColumnSchema;

import java.io.Serializable;
import java.util.List;

/**
 * Factory for creating {@link ColumnSchema}s to be used when creating a table that
 * does not currently exist in Kudu. Usable through {@link KuduTableInfo#createTableIfNotExists}.
 *
 * <p> This factory implementation must be Serializable as it will be used directly in the Flink sources
 * and sinks.
 */
/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public interface ColumnSchemasFactory extends Serializable {

    /**
     * Creates the columns of the Kudu table that will be used during the createTable operation.
     *
     * @return List of columns.
     */
    List<ColumnSchema> getColumnSchemas();

}
