package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class RiskWindowSummary implements Serializable {
    private String accountId;
    private long windowStartEpochMillis;
    private long windowEndEpochMillis;
    private long transactionCount;
    private long declinedCount;
    private long totalAmountMinor;
    private long maxAmountMinor;

    public RiskWindowSummary() {}

    public RiskWindowSummary(String accountId, long windowStartEpochMillis, long windowEndEpochMillis,
                             long transactionCount, long declinedCount, long totalAmountMinor, long maxAmountMinor) {
        this.accountId = accountId;
        this.windowStartEpochMillis = windowStartEpochMillis;
        this.windowEndEpochMillis = windowEndEpochMillis;
        this.transactionCount = transactionCount;
        this.declinedCount = declinedCount;
        this.totalAmountMinor = totalAmountMinor;
        this.maxAmountMinor = maxAmountMinor;
    }

    public String getAccountId() { return accountId; }
    public void setAccountId(String value) { this.accountId = value; }
    public long getWindowStartEpochMillis() { return windowStartEpochMillis; }
    public void setWindowStartEpochMillis(long value) { this.windowStartEpochMillis = value; }
    public long getWindowEndEpochMillis() { return windowEndEpochMillis; }
    public void setWindowEndEpochMillis(long value) { this.windowEndEpochMillis = value; }
    public long getTransactionCount() { return transactionCount; }
    public void setTransactionCount(long value) { this.transactionCount = value; }
    public long getDeclinedCount() { return declinedCount; }
    public void setDeclinedCount(long value) { this.declinedCount = value; }
    public long getTotalAmountMinor() { return totalAmountMinor; }
    public void setTotalAmountMinor(long value) { this.totalAmountMinor = value; }
    public long getMaxAmountMinor() { return maxAmountMinor; }
    public void setMaxAmountMinor(long value) { this.maxAmountMinor = value; }
}
