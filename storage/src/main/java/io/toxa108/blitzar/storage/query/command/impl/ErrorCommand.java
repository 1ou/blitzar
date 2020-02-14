package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.ErrorResultQuery;

public class ErrorCommand implements SqlCommand {
    @Override
    public byte[] execute(UserContext userContext, String[] sql) {
        return new ErrorResultQuery().toBytes();
    }
}
