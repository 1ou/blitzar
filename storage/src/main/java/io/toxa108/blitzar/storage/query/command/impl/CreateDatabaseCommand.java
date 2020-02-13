package io.toxa108.blitzar.storage.query.command.impl;

import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.command.SqlCommand;
import io.toxa108.blitzar.storage.query.impl.DataDefinitionQuery;

public class CreateDatabaseCommand implements SqlCommand {
    private final DataDefinitionQueryResolver dataDefinitionQueryResolver;

    public CreateDatabaseCommand(final DataDefinitionQueryResolver dataDefinitionQueryResolver) {
        this.dataDefinitionQueryResolver = dataDefinitionQueryResolver;
    }

    @Override
    public byte[] execute(final UserContext userContext, final String[] sql) {
        final DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                sql[2], DataDefinitionQuery.Type.CREATE_DATABASE
        );
        return dataDefinitionQueryResolver.createDatabase(dataDefinitionQuery).toBytes();
    }
}
