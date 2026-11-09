package com.bankstream.fraud.generator;

import com.bankstream.fraud.domain.model.*;
import com.bankstream.fraud.serialization.JsonCodec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

/**
 * Deterministic-ish data builder used by Docker labs. Builder methods keep
 * scenario construction separate from Kafka infrastructure concerns.
 */
public final class ScenarioProducer {
    private static final String TRANSACTIONS = "bank-transactions";
    private static final String RULES = "fraud-rules";
    private final Producer<String, String> producer;

    private ScenarioProducer(Producer<String, String> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        String scenario = args.length == 0 ? "normal" : args[0].toLowerCase(Locale.ROOT);
        try (Producer<String, String> kafka = new KafkaProducer<>(properties())) {
            ScenarioProducer runner = new ScenarioProducer(kafka);
            runner.seedDefaultRules();
            Thread.sleep(1_000);
            runner.run(scenario);
            kafka.flush();
            System.out.println("Published scenario '" + scenario + "' at " + Instant.now());
        }
    }

    private void run(String scenario) throws Exception {
        long now = System.currentTimeMillis();
        switch (scenario) {
            case "normal" -> send(tx("normal", "acc-101", 12_500, "TR", now));
            case "country" -> send(tx("country", "acc-102", 8_000, "US", now));
            case "duplicate" -> {
                TransactionEvent event = tx("same-id", "acc-103", 20_000, "TR", now);
                send(event);
                send(event);
            }
            case "invalid" -> {
                producer.send(new ProducerRecord<>(TRANSACTIONS, "bad-account",
                        "{\"transactionId\":\"bad-1\",\"accountId\":\"\",\"amountMinor\":-50}"));
                producer.send(new ProducerRecord<>(TRANSACTIONS, "broken", "{not-json"));
            }
            case "burst" -> {
                long burstTime = now - 70_000;
                for (int i = 0; i < 7; i++) {
                    send(tx("burst-" + i, "acc-burst", 3_000 + i, "TR", burstTime + i * 700L));
                }
                send(tx("watermark-anchor", "acc-burst", 1_000, "TR", now));
            }
            case "late" -> {
                send(tx("watermark-anchor", "acc-late", 1_000, "TR", now));
                Thread.sleep(7_000); // Let the partition watermark and idle detection advance.
                send(tx("too-late", "acc-late", 2_000, "TR", now - 180_000));
            }
            case "rule-update" -> {
                sendRule(new RuleConfig("high-amount", RuleType.HIGH_AMOUNT, true,
                        10_000, null, 2, 0));
                Thread.sleep(1_000);
                send(tx("after-rule-update", "acc-104", 15_000, "TR", now));
            }
            default -> throw new IllegalArgumentException(
                    "Unknown scenario. Use one of: " + List.of("normal", "country", "duplicate", "invalid", "burst", "late", "rule-update"));
        }
    }

    private void seedDefaultRules() {
        sendRule(new RuleConfig("high-amount", RuleType.HIGH_AMOUNT, true,
                100_000, null, 1, 0));
        sendRule(new RuleConfig("home-country", RuleType.COUNTRY_MISMATCH, true,
                0, "TR", 1, 0));
        producer.flush();
    }

    private void send(TransactionEvent event) {
        producer.send(new ProducerRecord<>(TRANSACTIONS, event.getAccountId(), JsonCodec.writeString(event)));
    }

    private void sendRule(RuleConfig rule) {
        producer.send(new ProducerRecord<>(RULES, rule.getRuleId(), JsonCodec.writeString(rule)));
    }

    private TransactionEvent tx(String label, String accountId, long amountMinor, String country, long eventTime) {
        return new TransactionEvent(label + "-" + UUID.randomUUID(), accountId, amountMinor,
                "TRY", country, TransactionChannel.MOBILE, TransactionStatus.APPROVED, eventTime);
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "fraud-lab-scenario-producer");
        return properties;
    }
}
