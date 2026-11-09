package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class VelocityAccumulator implements Serializable {
    private long count;
    private long totalAmountMinor;

    public VelocityAccumulator() {}

    public void add(TransactionEvent event) {
        count++;
        totalAmountMinor += event.getAmountMinor();
    }

    public VelocityAccumulator merge(VelocityAccumulator other) {
        count += other.count;
        totalAmountMinor += other.totalAmountMinor;
        return this;
    }

    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
    public long getTotalAmountMinor() { return totalAmountMinor; }
    public void setTotalAmountMinor(long value) { this.totalAmountMinor = value; }
}
