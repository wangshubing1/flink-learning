package com.king.kudu.serde;

import com.king.kudu.connector.KuduRow;
import org.apache.kudu.Schema;

/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public class DefaultSerDe implements KuduSerialization<KuduRow>, KuduDeserialization<KuduRow>{

    @Override
    public KuduRow deserialize(KuduRow row) {
        return row;
    }

    @Override
    public KuduRow serialize(KuduRow value) {
        return value;
    }

    @Override
    public KuduSerialization<KuduRow> withSchema(Schema schema) {
        return this;
    }
}
