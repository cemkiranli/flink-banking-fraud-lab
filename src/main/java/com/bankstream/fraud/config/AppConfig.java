package com.bankstream.fraud.config;

import java.io.Serializable;
import java.time.Duration;

public record AppConfig(String bootstrapServers, Duration maxOutOfOrderness,
                        Duration allowedLateness, int velocityThreshold) implements Serializable {
    public static AppConfig fromEnvironment() {
        return new AppConfig(
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
                Duration.ofSeconds(readLong("MAX_OUT_OF_ORDER_SECONDS", 5)),
                Duration.ofSeconds(readLong("ALLOWED_LATENESS_SECONDS", 30)),
                (int) readLong("VELOCITY_THRESHOLD", 5));
    }

    private static long readLong(String name, long fallback) {
        String raw = System.getenv(name);
        return raw == null ? fallback : Long.parseLong(raw);
    }
}
