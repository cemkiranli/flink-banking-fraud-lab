package com.bankstream.fraud.infrastructure.kafka;

import com.bankstream.fraud.serialization.JsonSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.io.Serializable;

public final class KafkaSinkFactory implements Serializable {
    private final String bootstrapServers;

    public KafkaSinkFactory(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public <T> KafkaSink<T> exactlyOnceJsonSink(String topic) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("fraud-lab-" + topic + "-")
                .setProperty("transaction.timeout.ms", "900000")
                .build();
    }
}
