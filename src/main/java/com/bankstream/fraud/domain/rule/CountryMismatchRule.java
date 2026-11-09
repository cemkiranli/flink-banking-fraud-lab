package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.domain.model.TransactionEvent;

import java.util.Optional;

public final class CountryMismatchRule extends AbstractFraudRule {
    @Override
    public Optional<FraudAlert> evaluate(TransactionEvent event, RuleConfig config, long detectedAtEpochMillis) {
        boolean isMismatch = config.isEnabled()
                && event.getEventTimeEpochMillis() >= config.getEffectiveFromEpochMillis()
                && config.getExpectedCountry() != null
                && !config.getExpectedCountry().equalsIgnoreCase(event.getCountry());
        return isMismatch
                ? Optional.of(alert(event, config,
                    "Transaction country " + event.getCountry() + " differs from expected " + config.getExpectedCountry(),
                    "MEDIUM", detectedAtEpochMillis))
                : Optional.empty();
    }
}
