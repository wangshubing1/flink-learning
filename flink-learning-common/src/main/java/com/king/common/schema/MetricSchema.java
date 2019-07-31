package com.king.common.schema;

import com.google.gson.Gson;
import com.king.common.model.MetricEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;
import java.nio.charset.Charset;


/**
 * @Author: king
 * @Date: 2019-07-31
 * @Desc: TODO
 */

public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {
    private static final Gson gson = new Gson();
    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }

    @Override
    public byte[] serialize(MetricEvent element) {
        return gson.toJson(element).getBytes(Charset.forName("UTF-8"));
    }
}
