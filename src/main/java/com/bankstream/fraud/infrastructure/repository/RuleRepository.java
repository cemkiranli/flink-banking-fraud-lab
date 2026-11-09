package com.bankstream.fraud.infrastructure.repository;

import com.bankstream.fraud.domain.model.RuleConfig;

/** Repository pattern isolates Flink state access from rule evaluation. */
public interface RuleRepository {
    Iterable<RuleConfig> findAll() throws Exception;
    void saveIfNewer(RuleConfig rule) throws Exception;
}
