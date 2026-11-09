package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class DuplicateEvent implements Serializable {
    private String transactionId;
    private String accountId;
    private long eventTimeEpochMillis;
    private long observedAtEpochMillis;

    public DuplicateEvent() {}

    public DuplicateEvent(String transactionId, String accountId, long eventTimeEpochMillis, long observedAtEpochMillis) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.eventTimeEpochMillis = eventTimeEpochMillis;
        this.observedAtEpochMillis = observedAtEpochMillis;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String value) { this.transactionId = value; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String value) { this.accountId = value; }
    public long getEventTimeEpochMillis() { return eventTimeEpochMillis; }
    public void setEventTimeEpochMillis(long value) { this.eventTimeEpochMillis = value; }
    public long getObservedAtEpochMillis() { return observedAtEpochMillis; }
    public void setObservedAtEpochMillis(long value) { this.observedAtEpochMillis = value; }
}
