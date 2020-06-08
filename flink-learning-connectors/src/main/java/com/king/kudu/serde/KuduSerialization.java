package com.king.kudu.serde;

import com.king.kudu.connector.KuduRow;
import org.apache.kudu.Schema;

/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public interface KuduSerialization<T> {
    KuduRow serialize(T value);

    KuduSerialization<T> withSchema(Schema schema);
}
