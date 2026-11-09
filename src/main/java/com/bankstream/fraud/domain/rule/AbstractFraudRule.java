package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.domain.model.TransactionEvent;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public abstract class AbstractFraudRule implements FraudRule {
    protected FraudAlert alert(TransactionEvent event, RuleConfig config, String reason, String severity,
                               long detectedAtEpochMillis) {
        String naturalKey = config.getRuleId() + ":" + config.getVersion() + ":" + event.getTransactionId();
        String alertId = UUID.nameUUIDFromBytes(naturalKey.getBytes(StandardCharsets.UTF_8)).toString();
        return new FraudAlert(alertId, config.getRuleId(), event.getAccountId(), event.getTransactionId(),
                reason, severity, event.getEventTimeEpochMillis(), detectedAtEpochMillis);
    }
}
