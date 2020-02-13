package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;

public class CreateIndexCommand implements SqlCommand {
    private final DataDefinitionQueryResolver dataDefinitionQueryResolver;

    public CreateIndexCommand(DataDefinitionQueryResolver dataDefinitionQueryResolver) {
        this.dataDefinitionQueryResolver = dataDefinitionQueryResolver;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        return new byte[0];
    }
}
