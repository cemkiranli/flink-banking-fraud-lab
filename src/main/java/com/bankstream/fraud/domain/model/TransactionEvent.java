package com.bankstream.fraud.domain.model;

import java.io.Serializable;

/**
 * Monetary values use minor units (kuruş/cents). Floating point money would
 * create rounding bugs and unstable comparisons in fraud rules.
 */
public class TransactionEvent implements Serializable {
    private String transactionId;
    private String accountId;
    private long amountMinor;
    private String currency;
    private String country;
    private TransactionChannel channel;
    private TransactionStatus status;
    private long eventTimeEpochMillis;

    public TransactionEvent() {
        // Required by Flink's POJO serializer and Jackson.
    }

    public TransactionEvent(String transactionId, String accountId, long amountMinor,
                            String currency, String country, TransactionChannel channel,
                            TransactionStatus status, long eventTimeEpochMillis) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.country = country;
        this.channel = channel;
        this.status = status;
        this.eventTimeEpochMillis = eventTimeEpochMillis;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public long getAmountMinor() { return amountMinor; }
    public void setAmountMinor(long amountMinor) { this.amountMinor = amountMinor; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    public TransactionChannel getChannel() { return channel; }
    public void setChannel(TransactionChannel channel) { this.channel = channel; }
    public TransactionStatus getStatus() { return status; }
    public void setStatus(TransactionStatus status) { this.status = status; }
    public long getEventTimeEpochMillis() { return eventTimeEpochMillis; }
    public void setEventTimeEpochMillis(long eventTimeEpochMillis) { this.eventTimeEpochMillis = eventTimeEpochMillis; }
}
