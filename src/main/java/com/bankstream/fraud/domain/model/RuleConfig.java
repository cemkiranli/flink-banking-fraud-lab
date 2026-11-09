package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class RuleConfig implements Serializable {
    private String ruleId;
    private RuleType type;
    private boolean enabled;
    private long thresholdMinor;
    private String expectedCountry;
    private long version;
    private long effectiveFromEpochMillis;

    public RuleConfig() {}

    public RuleConfig(String ruleId, RuleType type, boolean enabled, long thresholdMinor,
                      String expectedCountry, long version, long effectiveFromEpochMillis) {
        this.ruleId = ruleId;
        this.type = type;
        this.enabled = enabled;
        this.thresholdMinor = thresholdMinor;
        this.expectedCountry = expectedCountry;
        this.version = version;
        this.effectiveFromEpochMillis = effectiveFromEpochMillis;
    }

    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }
    public RuleType getType() { return type; }
    public void setType(RuleType type) { this.type = type; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public long getThresholdMinor() { return thresholdMinor; }
    public void setThresholdMinor(long thresholdMinor) { this.thresholdMinor = thresholdMinor; }
    public String getExpectedCountry() { return expectedCountry; }
    public void setExpectedCountry(String expectedCountry) { this.expectedCountry = expectedCountry; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public long getEffectiveFromEpochMillis() { return effectiveFromEpochMillis; }
    public void setEffectiveFromEpochMillis(long value) { this.effectiveFromEpochMillis = value; }
}
