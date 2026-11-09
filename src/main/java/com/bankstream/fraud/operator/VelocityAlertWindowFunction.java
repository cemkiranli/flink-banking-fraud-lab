package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.VelocityAccumulator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class VelocityAlertWindowFunction
        extends ProcessWindowFunction<VelocityAccumulator, FraudAlert, String, TimeWindow> {
    private final int threshold;

    public VelocityAlertWindowFunction(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void process(String accountId, Context context, Iterable<VelocityAccumulator> values,
                        Collector<FraudAlert> out) {
        VelocityAccumulator value = values.iterator().next();
        if (value.getCount() < threshold) return;

        String naturalKey = "VELOCITY:" + accountId + ":" + context.window().getStart();
        String alertId = UUID.nameUUIDFromBytes(naturalKey.getBytes(StandardCharsets.UTF_8)).toString();
        out.collect(new FraudAlert(alertId, "VELOCITY_60S", accountId, null,
                value.getCount() + " transactions in a 60 second sliding window; total="
                        + value.getTotalAmountMinor(),
                "HIGH", context.window().getEnd() - 1, System.currentTimeMillis()));
    }
}
