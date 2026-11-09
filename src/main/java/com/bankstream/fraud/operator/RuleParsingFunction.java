package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.serialization.JsonCodec;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public final class RuleParsingFunction extends RichFlatMapFunction<String, RuleConfig> {
    @Override
    public void flatMap(String raw, Collector<RuleConfig> out) throws Exception {
        RuleConfig rule = JsonCodec.read(raw, RuleConfig.class);
        if (rule.getRuleId() == null || rule.getRuleId().isBlank() || rule.getType() == null || rule.getVersion() <= 0) {
            throw new IllegalArgumentException("Invalid rule config: " + raw);
        }
        out.collect(rule);
    }
}
