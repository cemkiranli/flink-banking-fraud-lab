package com.bankstream.fraud.operator;

import com.bankstream.fraud.config.OutputTags;
import com.bankstream.fraud.domain.model.RejectedEvent;
import com.bankstream.fraud.domain.model.TransactionEvent;
import com.bankstream.fraud.serialization.JsonCodec;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

public final class TransactionParsingFunction extends ProcessFunction<String, TransactionEvent> {
    private final TransactionValidator validator = new TransactionValidator();

    @Override
    public void processElement(String raw, Context context, Collector<TransactionEvent> out) {
        try {
            TransactionEvent event = JsonCodec.read(raw, TransactionEvent.class);
            Optional<String> error = validator.validate(event);
            if (error.isPresent()) {
                context.output(OutputTags.INVALID_EVENTS,
                        new RejectedEvent(raw, error.get(), System.currentTimeMillis()));
            } else {
                out.collect(event);
            }
        } catch (Exception e) {
            context.output(OutputTags.INVALID_EVENTS,
                    new RejectedEvent(raw, "malformed JSON: " + e.getMessage(), System.currentTimeMillis()));
        }
    }
}
