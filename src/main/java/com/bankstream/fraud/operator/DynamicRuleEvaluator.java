package com.bankstream.fraud.operator;

import com.bankstream.fraud.config.OutputTags;
import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.domain.model.TransactionEvent;
import com.bankstream.fraud.domain.rule.FraudRuleFactory;
import com.bankstream.fraud.infrastructure.repository.BroadcastStateRuleRepository;
import com.bankstream.fraud.infrastructure.repository.RuleRepository;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public final class DynamicRuleEvaluator
        extends KeyedBroadcastProcessFunction<String, TransactionEvent, RuleConfig, TransactionEvent> {
    private final MapStateDescriptor<String, RuleConfig> descriptor;
    private final FraudRuleFactory factory = new FraudRuleFactory();

    public DynamicRuleEvaluator(MapStateDescriptor<String, RuleConfig> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void processElement(TransactionEvent event, ReadOnlyContext context,
                               Collector<TransactionEvent> out) throws Exception {
        RuleRepository repository = new BroadcastStateRuleRepository(context.getBroadcastState(descriptor));
        for (RuleConfig config : repository.findAll()) {
            Optional<FraudAlert> alert = factory.create(config.getType())
                    .evaluate(event, config, System.currentTimeMillis());
            alert.ifPresent(value -> context.output(OutputTags.RULE_ALERTS, value));
        }
        out.collect(event);
    }

    @Override
    public void processBroadcastElement(RuleConfig rule, Context context,
                                        Collector<TransactionEvent> out) throws Exception {
        RuleRepository repository = new BroadcastStateRuleRepository(context.getBroadcastState(descriptor));
        repository.saveIfNewer(rule);
    }
}
