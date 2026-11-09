package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.RiskAccumulator;
import com.bankstream.fraud.domain.model.TransactionEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class RiskAggregateFunction
        implements AggregateFunction<TransactionEvent, RiskAccumulator, RiskAccumulator> {
    @Override
    public RiskAccumulator createAccumulator() { return new RiskAccumulator(); }

    @Override
    public RiskAccumulator add(TransactionEvent event, RiskAccumulator accumulator) {
        accumulator.add(event);
        return accumulator;
    }

    @Override
    public RiskAccumulator getResult(RiskAccumulator accumulator) { return accumulator; }

    @Override
    public RiskAccumulator merge(RiskAccumulator left, RiskAccumulator right) { return left.merge(right); }
}
