package com.king.kudu.serde;

import com.king.kudu.connector.KuduRow;

import java.io.Serializable;

/**
 * @Author: king
 * @Date: 2019-08-27
 * @Desc: TODO
 */

public interface KuduDeserialization<T> extends Serializable {
    T deserialize(KuduRow row);
}
