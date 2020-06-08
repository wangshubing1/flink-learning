package com.king.kudu.connector;


import org.apache.commons.lang3.Validate;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;

import java.io.Serializable;


/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public class KuduTableInfo implements Serializable {
    private String name;
    private CreateTableOptionsFactory createTableOptionsFactory = null;
    private ColumnSchemasFactory schemasFactory = null;

    private KuduTableInfo(String name) {
        this.name = Validate.notNull(name);
    }

    /**
     * Creates a new {@link KuduTableInfo} that is sufficient for reading/writing to existing Kudu Tables.
     * For creating new tables call {@link #createTableIfNotExists} afterwards.
     *
     * @param name Table name in Kudu
     * @return KuduTableInfo for the given table name
     */
    public static KuduTableInfo forTable(String name) {
        return new KuduTableInfo(name);
    }

    /**
     * Defines table parameters to be used when creating the Kudu table if it does not exist (read or write)
     *
     * @param schemasFactory            factory for defining columns
     * @param createTableOptionsFactory factory for defining create table options
     * @return KuduTableInfo that will create tables that does not exist with the given settings.
     */
    public KuduTableInfo createTableIfNotExists(ColumnSchemasFactory schemasFactory, CreateTableOptionsFactory createTableOptionsFactory) {
        this.createTableOptionsFactory = Validate.notNull(createTableOptionsFactory);
        this.schemasFactory = Validate.notNull(schemasFactory);
        return this;
    }

    /**
     * Returns the {@link Schema} of the table. Only works if {@link #createTableIfNotExists} was specified otherwise throws an error.
     *
     * @return Schema of the target table.
     */
    public Schema getSchema() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access schema for KuduTableInfo. Use createTableIfNotExists to specify the columns.");
        }

        return new Schema(schemasFactory.getColumnSchemas());
    }

    /**
     * @return Name of the table.
     */
    public String getName() {
        return name;
    }

    /**
     * @return True if table creation is enabled if target table does not exist.
     */
    public boolean getCreateTableIfNotExists() {
        return createTableOptionsFactory != null;
    }

    /**
     * @return CreateTableOptions if {@link #createTableIfNotExists} was specified.
     */
    public CreateTableOptions getCreateTableOptions() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access CreateTableOptions for KuduTableInfo. Use createTableIfNotExists to specify.");
        }
        return createTableOptionsFactory.getCreateTableOptions();
    }
}
