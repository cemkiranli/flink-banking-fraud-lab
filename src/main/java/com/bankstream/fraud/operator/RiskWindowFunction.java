package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.RiskAccumulator;
import com.bankstream.fraud.domain.model.RiskWindowSummary;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class RiskWindowFunction
        extends ProcessWindowFunction<RiskAccumulator, RiskWindowSummary, String, TimeWindow> {
    @Override
    public void process(String accountId, Context context, Iterable<RiskAccumulator> values,
                        Collector<RiskWindowSummary> out) {
        RiskAccumulator value = values.iterator().next();
        out.collect(new RiskWindowSummary(accountId, context.window().getStart(), context.window().getEnd(),
                value.getTransactionCount(), value.getDeclinedCount(), value.getTotalAmountMinor(),
                value.getMaxAmountMinor()));
    }
}
