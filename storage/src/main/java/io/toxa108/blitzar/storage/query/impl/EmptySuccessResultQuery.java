package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.query.ResultQuery;

public class EmptySuccessResultQuery implements ResultQuery {
    @Override
    public byte[] toBytes() {
        return "success.".getBytes();
    }
}
