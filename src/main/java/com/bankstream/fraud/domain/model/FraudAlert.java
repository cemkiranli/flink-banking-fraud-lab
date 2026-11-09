package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class FraudAlert implements Serializable {
    private String alertId;
    private String ruleId;
    private String accountId;
    private String transactionId;
    private String reason;
    private String severity;
    private long eventTimeEpochMillis;
    private long detectedAtEpochMillis;

    public FraudAlert() {}

    public FraudAlert(String alertId, String ruleId, String accountId, String transactionId,
                      String reason, String severity, long eventTimeEpochMillis, long detectedAtEpochMillis) {
        this.alertId = alertId;
        this.ruleId = ruleId;
        this.accountId = accountId;
        this.transactionId = transactionId;
        this.reason = reason;
        this.severity = severity;
        this.eventTimeEpochMillis = eventTimeEpochMillis;
        this.detectedAtEpochMillis = detectedAtEpochMillis;
    }

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    public long getEventTimeEpochMillis() { return eventTimeEpochMillis; }
    public void setEventTimeEpochMillis(long value) { this.eventTimeEpochMillis = value; }
    public long getDetectedAtEpochMillis() { return detectedAtEpochMillis; }
    public void setDetectedAtEpochMillis(long value) { this.detectedAtEpochMillis = value; }
}
