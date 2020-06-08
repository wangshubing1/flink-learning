
package com.king.kudu.connector.failure;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.RowError;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public interface KuduFailureHandler extends Serializable {

    void onFailure(List<RowError> failure) throws IOException;

    default void onTypeMismatch(ClassCastException e) throws IOException {
        throw new IOException("Class casting failed \n", e);
    }
}
