
package com.king.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@PublicEvolving
public interface KuduOperationMapper<T> extends Serializable {

    List<Operation> createOperations(T input, KuduTable table);

}
