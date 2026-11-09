package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.TransactionEvent;
import com.bankstream.fraud.domain.model.VelocityAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class VelocityAggregateFunction
        implements AggregateFunction<TransactionEvent, VelocityAccumulator, VelocityAccumulator> {
    @Override
    public VelocityAccumulator createAccumulator() { return new VelocityAccumulator(); }

    @Override
    public VelocityAccumulator add(TransactionEvent event, VelocityAccumulator accumulator) {
        accumulator.add(event);
        return accumulator;
    }

    @Override
    public VelocityAccumulator getResult(VelocityAccumulator accumulator) { return accumulator; }

    @Override
    public VelocityAccumulator merge(VelocityAccumulator left, VelocityAccumulator right) {
        return left.merge(right);
    }
}
