package com.bankstream.fraud.application;

import com.bankstream.fraud.config.AppConfig;
import com.bankstream.fraud.config.OutputTags;
import com.bankstream.fraud.domain.model.*;
import com.bankstream.fraud.infrastructure.kafka.KafkaSinkFactory;
import com.bankstream.fraud.infrastructure.kafka.KafkaSourceFactory;
import com.bankstream.fraud.operator.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class FraudDetectionJob {
    private FraudDetectionJob() {}

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureFaultTolerance(env);

        KafkaSourceFactory sourceFactory = new KafkaSourceFactory(config.bootstrapServers());
        KafkaSinkFactory sinkFactory = new KafkaSinkFactory(config.bootstrapServers());

        SingleOutputStreamOperator<TransactionEvent> parsed = env
                .fromSource(sourceFactory.stringSource("bank-transactions", "fraud-detector-v1"),
                        WatermarkStrategy.noWatermarks(), "Kafka transactions")
                .uid("transaction-source-v1")
                .process(new TransactionParsingFunction())
                .name("validate and parse transactions")
                .uid("transaction-parser-v1");

        DataStream<RejectedEvent> invalidEvents = parsed.getSideOutput(OutputTags.INVALID_EVENTS);

        SingleOutputStreamOperator<TransactionEvent> watermarked = parsed
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(config.maxOutOfOrderness())
                                .withTimestampAssigner((event, ignored) -> event.getEventTimeEpochMillis())
                                .withIdleness(java.time.Duration.ofSeconds(5)))
                .name("event-time and bounded-out-of-orderness watermarks")
                .uid("watermarks-v1");

        SingleOutputStreamOperator<TransactionEvent> deduplicated = watermarked
                .keyBy(TransactionEvent::getTransactionId)
                .process(new DeduplicationFunction())
                .name("24h idempotency filter")
                .uid("deduplication-v1");

        DataStream<DuplicateEvent> duplicateEvents = deduplicated.getSideOutput(OutputTags.DUPLICATE_EVENTS);

        MapStateDescriptor<String, RuleConfig> ruleStateDescriptor = new MapStateDescriptor<>(
                "dynamic-fraud-rules", Types.STRING, TypeInformation.of(RuleConfig.class));

        BroadcastStream<RuleConfig> rules = env
                .fromSource(sourceFactory.stringSource("fraud-rules", "fraud-rules-v1"),
                        WatermarkStrategy.noWatermarks(), "Kafka fraud rules")
                .uid("rule-source-v1")
                .flatMap(new RuleParsingFunction())
                .name("parse rule updates")
                .uid("rule-parser-v1")
                .broadcast(ruleStateDescriptor);

        SingleOutputStreamOperator<TransactionEvent> evaluated = deduplicated
                .keyBy(TransactionEvent::getAccountId)
                .connect(rules)
                .process(new DynamicRuleEvaluator(ruleStateDescriptor))
                .name("broadcast-state dynamic rule evaluation")
                .uid("dynamic-rule-evaluator-v1");

        DataStream<FraudAlert> ruleAlerts = evaluated.getSideOutput(OutputTags.RULE_ALERTS);

        SingleOutputStreamOperator<RiskWindowSummary> summaries = evaluated
                .keyBy(TransactionEvent::getAccountId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.milliseconds(config.allowedLateness().toMillis()))
                .sideOutputLateData(OutputTags.LATE_EVENTS)
                .aggregate(new RiskAggregateFunction(), new RiskWindowFunction())
                .name("one-minute account risk windows")
                .uid("risk-window-v1");

        DataStream<TransactionEvent> lateEvents = summaries.getSideOutput(OutputTags.LATE_EVENTS);

        DataStream<FraudAlert> velocityAlerts = evaluated
                .keyBy(TransactionEvent::getAccountId)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .allowedLateness(Time.milliseconds(config.allowedLateness().toMillis()))
                .aggregate(new VelocityAggregateFunction(),
                        new VelocityAlertWindowFunction(config.velocityThreshold()))
                .name("sliding-window velocity detector")
                .uid("velocity-window-v1");

        ruleAlerts.union(velocityAlerts)
                .sinkTo(sinkFactory.exactlyOnceJsonSink("fraud-alerts"))
                .name("exactly-once fraud alerts")
                .uid("fraud-alert-sink-v1");
        summaries.sinkTo(sinkFactory.exactlyOnceJsonSink("risk-window-summaries"))
                .name("exactly-once risk summaries").uid("risk-summary-sink-v1");
        invalidEvents.sinkTo(sinkFactory.exactlyOnceJsonSink("invalid-events"))
                .name("invalid event DLQ").uid("invalid-event-sink-v1");
        duplicateEvents.sinkTo(sinkFactory.exactlyOnceJsonSink("duplicate-events"))
                .name("duplicate audit sink").uid("duplicate-event-sink-v1");
        lateEvents.sinkTo(sinkFactory.exactlyOnceJsonSink("late-events"))
                .name("too-late event sink").uid("late-event-sink-v1");

        env.execute("Banking Fraud Detection - Event Time Lab");
    }

    private static void configureFaultTolerance(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setAutoWatermarkInterval(1_000);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(5_000);
        checkpoints.setCheckpointTimeout(60_000);
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setTolerableCheckpointFailureNumber(2);
        checkpoints.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
