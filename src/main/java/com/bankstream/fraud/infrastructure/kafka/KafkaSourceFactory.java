package com.bankstream.fraud.infrastructure.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.Serializable;

public final class KafkaSourceFactory implements Serializable {
    private final String bootstrapServers;

    public KafkaSourceFactory(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public KafkaSource<String> stringSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
