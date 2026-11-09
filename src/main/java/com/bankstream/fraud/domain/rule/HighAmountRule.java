package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.domain.model.TransactionEvent;

import java.util.Optional;

public final class HighAmountRule extends AbstractFraudRule {
    @Override
    public Optional<FraudAlert> evaluate(TransactionEvent event, RuleConfig config, long detectedAtEpochMillis) {
        if (!config.isEnabled() || event.getEventTimeEpochMillis() < config.getEffectiveFromEpochMillis()
                || event.getAmountMinor() < config.getThresholdMinor()) {
            return Optional.empty();
        }
        return Optional.of(alert(event, config,
                "Amount " + event.getAmountMinor() + " reached threshold " + config.getThresholdMinor(),
                "HIGH", detectedAtEpochMillis));
    }
}
