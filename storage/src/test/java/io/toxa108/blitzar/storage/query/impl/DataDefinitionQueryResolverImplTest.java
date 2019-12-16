package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.DatabaseContextImpl;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataDefinitionQueryResolverImplTest {
    @Test
    public void create_database_when_success() {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");
        DatabaseContext databaseContext = new DatabaseContextImpl(fileManager);
        DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                "databasee", DataDefinitionQuery.Type.CREATE_DATABASE);

        dataDefinitionQueryResolver.createDatabase(dataDefinitionQuery);
        assertEquals(1, fileManager.databases().size());
        assertEquals(databaseContext.databases().get(0).name(), fileManager.databases().get(0));
    }

    @Test
    public void create_database_and_table_when_success() {
        String databaseName = "databasee";
        String tableName = "table";

        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");
        DatabaseContext databaseContext = new DatabaseContextImpl(fileManager);
        DataDefinitionQueryResolver dataDefinitionQueryResolver =
                new DataDefinitionQueryResolverImpl(databaseContext);

        DataDefinitionQuery dataDefinitionQuery = new DataDefinitionQuery(
                databaseName, DataDefinitionQuery.Type.CREATE_DATABASE);

        dataDefinitionQueryResolver.createDatabase(dataDefinitionQuery);

        assertEquals(databaseContext.databases().size(), fileManager.databases().size());
        databaseName = databaseContext.databases().get(0).name();

        assertEquals(databaseName, fileManager.databases().get(0));

        DataDefinitionQuery dataDefinitionQueryCreateTable = new DataDefinitionQuery(
                databaseName, tableName, List.of(), DataDefinitionQuery.Type.CREATE_TABLE);

        ResultQuery resultQuery = dataDefinitionQueryResolver.createTable(dataDefinitionQueryCreateTable);

        Assert.assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }
}