package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.TransactionChannel;
import com.bankstream.fraud.domain.model.TransactionEvent;
import com.bankstream.fraud.domain.model.TransactionStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionValidatorTest {
    private final TransactionValidator validator = new TransactionValidator();

    @Test
    void rejectsZeroAndNegativeMoney() {
        assertEquals("amountMinor must be positive", validator.validate(validEvent(0)).orElseThrow());
        assertEquals("amountMinor must be positive", validator.validate(validEvent(-1)).orElseThrow());
    }

    @Test
    void acceptsKnownCurrencyAndCompleteEvent() {
        assertTrue(validator.validate(validEvent(1)).isEmpty());
    }

    private TransactionEvent validEvent(long amount) {
        return new TransactionEvent("tx", "account", amount, "TRY", "TR",
                TransactionChannel.POS, TransactionStatus.APPROVED, 1);
    }
}
