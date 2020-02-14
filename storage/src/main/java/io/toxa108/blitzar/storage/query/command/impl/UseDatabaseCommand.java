package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;

import java.util.concurrent.ConcurrentHashMap;

public class UseDatabaseCommand implements SqlCommand {
    private final ConcurrentHashMap<String, String> usersActiveDatabases;

    public UseDatabaseCommand(final ConcurrentHashMap<String, String> usersActiveDatabases) {
        this.usersActiveDatabases = usersActiveDatabases;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        usersActiveDatabases.put(userContext.user().login(), sql[1]);
        return new EmptySuccessResultQuery().toBytes();
    }
}
