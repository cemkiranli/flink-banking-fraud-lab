package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class RiskAccumulator implements Serializable {
    private long transactionCount;
    private long declinedCount;
    private long totalAmountMinor;
    private long maxAmountMinor;

    public RiskAccumulator() {}

    public void add(TransactionEvent event) {
        transactionCount++;
        totalAmountMinor += event.getAmountMinor();
        maxAmountMinor = Math.max(maxAmountMinor, event.getAmountMinor());
        if (event.getStatus() == TransactionStatus.DECLINED) {
            declinedCount++;
        }
    }

    public RiskAccumulator merge(RiskAccumulator other) {
        transactionCount += other.transactionCount;
        declinedCount += other.declinedCount;
        totalAmountMinor += other.totalAmountMinor;
        maxAmountMinor = Math.max(maxAmountMinor, other.maxAmountMinor);
        return this;
    }

    public long getTransactionCount() { return transactionCount; }
    public void setTransactionCount(long value) { this.transactionCount = value; }
    public long getDeclinedCount() { return declinedCount; }
    public void setDeclinedCount(long value) { this.declinedCount = value; }
    public long getTotalAmountMinor() { return totalAmountMinor; }
    public void setTotalAmountMinor(long value) { this.totalAmountMinor = value; }
    public long getMaxAmountMinor() { return maxAmountMinor; }
    public void setMaxAmountMinor(long value) { this.maxAmountMinor = value; }
}
