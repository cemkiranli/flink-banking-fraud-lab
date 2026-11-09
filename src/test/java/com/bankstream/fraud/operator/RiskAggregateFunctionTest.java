package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RiskAggregateFunctionTest {
    @Test
    void aggregatesCountsAmountsDeclinesAndMaximumWithoutFloatingPoint() {
        RiskAggregateFunction aggregate = new RiskAggregateFunction();
        RiskAccumulator accumulator = aggregate.createAccumulator();
        aggregate.add(event(10_01, TransactionStatus.APPROVED), accumulator);
        aggregate.add(event(20_02, TransactionStatus.DECLINED), accumulator);

        assertEquals(2, accumulator.getTransactionCount());
        assertEquals(1, accumulator.getDeclinedCount());
        assertEquals(30_03, accumulator.getTotalAmountMinor());
        assertEquals(20_02, accumulator.getMaxAmountMinor());
    }

    private TransactionEvent event(long amount, TransactionStatus status) {
        return new TransactionEvent("tx-" + amount, "acc", amount, "TRY", "TR",
                TransactionChannel.ATM, status, 1);
    }
}
