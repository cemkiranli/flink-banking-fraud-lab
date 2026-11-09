package com.bankstream.fraud.infrastructure.repository;

import com.bankstream.fraud.domain.model.RuleConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class BroadcastStateRuleRepository implements RuleRepository {
    private final ReadOnlyBroadcastState<String, RuleConfig> readableState;
    private final BroadcastState<String, RuleConfig> writableState;

    public BroadcastStateRuleRepository(ReadOnlyBroadcastState<String, RuleConfig> state) {
        this.readableState = state;
        this.writableState = null;
    }

    public BroadcastStateRuleRepository(BroadcastState<String, RuleConfig> state) {
        this.readableState = state;
        this.writableState = state;
    }

    @Override
    public Iterable<RuleConfig> findAll() throws Exception {
        List<RuleConfig> rules = new ArrayList<>();
        for (Map.Entry<String, RuleConfig> entry : readableState.immutableEntries()) {
            rules.add(entry.getValue());
        }
        return rules;
    }

    @Override
    public void saveIfNewer(RuleConfig candidate) throws Exception {
        if (writableState == null) {
            throw new UnsupportedOperationException("Read-only repository");
        }
        RuleConfig current = writableState.get(candidate.getRuleId());
        if (current == null || candidate.getVersion() > current.getVersion()) {
            writableState.put(candidate.getRuleId(), candidate);
        }
    }
}
