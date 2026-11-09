package com.bankstream.fraud.config;

import com.bankstream.fraud.domain.model.DuplicateEvent;
import com.bankstream.fraud.domain.model.FraudAlert;
import com.bankstream.fraud.domain.model.RejectedEvent;
import com.bankstream.fraud.domain.model.TransactionEvent;
import org.apache.flink.util.OutputTag;

public final class OutputTags {
    public static final OutputTag<RejectedEvent> INVALID_EVENTS = new OutputTag<>("invalid-events") {};
    public static final OutputTag<DuplicateEvent> DUPLICATE_EVENTS = new OutputTag<>("duplicate-events") {};
    public static final OutputTag<TransactionEvent> LATE_EVENTS = new OutputTag<>("late-events") {};
    public static final OutputTag<FraudAlert> RULE_ALERTS = new OutputTag<>("rule-alerts") {};

    private OutputTags() {}
}
