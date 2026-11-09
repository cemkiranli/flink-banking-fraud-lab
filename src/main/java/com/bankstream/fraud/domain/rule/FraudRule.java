package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RuleConfig;
import com.bankstream.fraud.domain.model.TransactionEvent;

import java.io.Serializable;
import java.util.Optional;

/** Strategy pattern: every rule owns one fraud decision. */
public interface FraudRule extends Serializable {
    Optional<FraudAlert> evaluate(TransactionEvent event, RuleConfig config, long detectedAtEpochMillis);
}
