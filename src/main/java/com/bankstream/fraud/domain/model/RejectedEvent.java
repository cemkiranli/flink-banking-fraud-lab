package com.bankstream.fraud.domain.model;

import java.io.Serializable;

public class RejectedEvent implements Serializable {
    private String rawPayload;
    private String reason;
    private long rejectedAtEpochMillis;

    public RejectedEvent() {}

    public RejectedEvent(String rawPayload, String reason, long rejectedAtEpochMillis) {
        this.rawPayload = rawPayload;
        this.reason = reason;
        this.rejectedAtEpochMillis = rejectedAtEpochMillis;
    }

    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public long getRejectedAtEpochMillis() { return rejectedAtEpochMillis; }
    public void setRejectedAtEpochMillis(long value) { this.rejectedAtEpochMillis = value; }
}
