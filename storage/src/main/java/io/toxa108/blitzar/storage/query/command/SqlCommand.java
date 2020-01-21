package io.toxa108.blitzar.storage.query.command;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.query.UserContext;

public interface SqlCommand {
    /**
     * Execute sql command
     * @param userContext user context
     * @param sql sql request
     * @return bytes
     */
    byte[] execute(UserContext userContext, @NotNull final String[] sql);
}
