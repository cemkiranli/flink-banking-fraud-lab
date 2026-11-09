package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.RuleType;

import java.io.Serializable;
/** Factory pattern: operators depend on the abstraction, not concrete rules. */
public final class FraudRuleFactory implements Serializable {
    public FraudRule create(RuleType type) {
        if (type == null) throw new IllegalArgumentException("Rule type cannot be null");
        return switch (type) {
            case HIGH_AMOUNT -> new HighAmountRule();
            case COUNTRY_MISMATCH -> new CountryMismatchRule();
        };
    }
}
