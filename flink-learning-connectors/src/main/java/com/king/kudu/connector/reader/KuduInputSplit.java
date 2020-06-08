
package com.king.kudu.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.LocatableInputSplit;

/**
 * @Author: king
 * @Date: 2020-06-08
 * @Desc: TODO
 */
@Internal
public class KuduInputSplit extends LocatableInputSplit {

    private byte[] scanToken;
    public KuduInputSplit(byte[] scanToken, final int splitNumber, final String[] hostnames) {
        super(splitNumber, hostnames);

        this.scanToken = scanToken;
    }

    public byte[] getScanToken() {
        return scanToken;
    }
}
