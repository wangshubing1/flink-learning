
package com.king.kudu.connector.failure;

import org.apache.kudu.client.RowError;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
public class DefaultKuduFailureHandler implements KuduFailureHandler {

    @Override
    public void onFailure(List<RowError> failure) throws IOException {
        String errors = failure.stream()
                .map(error -> error.toString() + System.lineSeparator())
                .collect(Collectors.joining());

        throw new IOException("Error while sending value. \n " + errors);
    }
}
