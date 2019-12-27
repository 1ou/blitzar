package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.NotNull;

public class QueryProcessException extends Exception {
    public QueryProcessException(@NotNull final String message) {
        super(message);
    }
}
