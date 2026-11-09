package com.bankstream.fraud.operator;

import com.bankstream.fraud.config.OutputTags;
import com.bankstream.fraud.domain.model.DuplicateEvent;
import com.bankstream.fraud.domain.model.TransactionEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public final class DeduplicationFunction extends KeyedProcessFunction<String, TransactionEvent, TransactionEvent> {
    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1_000)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-transaction-id", Boolean.class);
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TransactionEvent event, Context context, Collector<TransactionEvent> out) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            context.output(OutputTags.DUPLICATE_EVENTS,
                    new DuplicateEvent(event.getTransactionId(), event.getAccountId(),
                            event.getEventTimeEpochMillis(), System.currentTimeMillis()));
            return;
        }
        seen.update(true);
        out.collect(event);
    }
}
