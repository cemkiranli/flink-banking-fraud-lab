package com.bankstream.fraud.domain.rule;

import com.bankstream.fraud.domain.model.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FraudRuleFactoryTest {
    private final FraudRuleFactory factory = new FraudRuleFactory();

    @Test
    void highAmountBoundaryIsInclusiveAndAlertIdIsDeterministic() {
        TransactionEvent event = event(100_000, "TR");
        RuleConfig config = new RuleConfig("high", RuleType.HIGH_AMOUNT, true,
                100_000, null, 4, 0);

        FraudAlert first = factory.create(RuleType.HIGH_AMOUNT).evaluate(event, config, 10).orElseThrow();
        FraudAlert retried = factory.create(RuleType.HIGH_AMOUNT).evaluate(event, config, 20).orElseThrow();

        assertEquals(first.getAlertId(), retried.getAlertId(), "retry must keep the idempotency key");
        assertEquals("HIGH", first.getSeverity());
    }

    @Test
    void olderEventDoesNotUseFutureRuleVersion() {
        TransactionEvent event = event(999_999, "TR");
        event.setEventTimeEpochMillis(99);
        RuleConfig futureRule = new RuleConfig("high", RuleType.HIGH_AMOUNT, true,
                1, null, 2, 100);

        assertTrue(factory.create(RuleType.HIGH_AMOUNT).evaluate(event, futureRule, 200).isEmpty());
    }

    @Test
    void countryComparisonIsCaseInsensitive() {
        RuleConfig config = new RuleConfig("country", RuleType.COUNTRY_MISMATCH, true,
                0, "tr", 1, 0);
        assertTrue(factory.create(RuleType.COUNTRY_MISMATCH).evaluate(event(100, "TR"), config, 1).isEmpty());
        assertTrue(factory.create(RuleType.COUNTRY_MISMATCH).evaluate(event(100, "DE"), config, 1).isPresent());
    }

    private TransactionEvent event(long amount, String country) {
        return new TransactionEvent("tx-1", "acc-1", amount, "TRY", country,
                TransactionChannel.WEB, TransactionStatus.APPROVED, 1_000);
    }
}
