package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.DatabaseContextImpl;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataDefinitionQueryResolverImplTest {
    @Test
    public void create_database_when_success() {
        FileManager fileManager = new FileManagerImpl();
        DatabaseContext databaseContext = new DatabaseContextImpl(fileManager);
        DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                "databasee", DataDefinitionQuery.Type.CREATE_DATABASE);

        dataDefinitionQueryResolver.createDatabase(dataDefinitionQuery);
        assertEquals(1, fileManager.databases().size());
        assertEquals("databasee", fileManager.databases().get(0));
    }
}