package com.bankstream.fraud.operator;

import com.bankstream.fraud.domain.model.TransactionEvent;

import java.io.Serializable;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class TransactionValidator implements Serializable {
    private static final Set<String> CURRENCIES = Set.of("TRY", "USD", "EUR", "GBP");

    public Optional<String> validate(TransactionEvent event) {
        if (isBlank(event.getTransactionId())) return Optional.of("transactionId is required");
        if (isBlank(event.getAccountId())) return Optional.of("accountId is required");
        if (event.getAmountMinor() <= 0) return Optional.of("amountMinor must be positive");
        if (isBlank(event.getCurrency()) || !CURRENCIES.contains(event.getCurrency().toUpperCase(Locale.ROOT))) {
            return Optional.of("currency must be one of " + CURRENCIES);
        }
        if (isBlank(event.getCountry()) || event.getCountry().length() != 2) {
            return Optional.of("country must be an ISO-3166 alpha-2 code");
        }
        if (event.getChannel() == null) return Optional.of("channel is required");
        if (event.getStatus() == null) return Optional.of("status is required");
        if (event.getEventTimeEpochMillis() <= 0) return Optional.of("eventTimeEpochMillis must be positive");
        return Optional.empty();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
