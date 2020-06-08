

package com.king.kudu.connector;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.client.CreateTableOptions;

import java.io.Serializable;

/**
 * Factory for creating {@link CreateTableOptions} to be used when creating a table that
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
public interface CreateTableOptionsFactory extends Serializable {

    /**
     * Creates the {@link CreateTableOptions} that will be used during the createTable operation.
     *
     * @return CreateTableOptions for creating the table.
     */
    CreateTableOptions getCreateTableOptions();

}
