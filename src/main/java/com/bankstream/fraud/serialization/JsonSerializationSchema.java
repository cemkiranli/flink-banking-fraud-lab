package com.bankstream.fraud.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

public final class JsonSerializationSchema<T> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(T element) {
        return JsonCodec.writeBytes(element);
    }
}
