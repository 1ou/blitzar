package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.DatabaseContext;
import io.toxa108.blitzar.storage.database.DatabaseContextImpl;
import io.toxa108.blitzar.storage.database.schema.impl.FieldImpl;
import io.toxa108.blitzar.storage.database.schema.impl.FieldType;
import io.toxa108.blitzar.storage.database.schema.impl.IndexImpl;
import io.toxa108.blitzar.storage.database.schema.impl.IndexType;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DataDefinitionQueryResolverImplTest {

    @Before
    public void before() {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");
        fileManager.clear();
    }

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
        assertEquals(databaseContext.databases().stream().findAny().orElseThrow().name(),
                fileManager.databases().get(0));
    }

    @Test
    public void create_database_and_empty_table_when_success() {
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
        databaseName = databaseContext.databases().stream().findAny().orElseThrow().name();

        assertEquals(databaseName, fileManager.databases().get(0));

        DataDefinitionQuery dataDefinitionQueryCreateTable = new DataDefinitionQuery(
                databaseName, tableName, Set.of(), Set.of(), DataDefinitionQuery.Type.CREATE_TABLE);

        ResultQuery resultQuery = dataDefinitionQueryResolver.createTable(dataDefinitionQueryCreateTable);

        Assert.assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }

    @Test
    public void create_database_and_table_with_scheme_when_success() {
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
        databaseName = databaseContext.databases().stream().findAny().orElseThrow().name();

        assertEquals(databaseName, fileManager.databases().get(0));

        DataDefinitionQuery dataDefinitionQueryCreateTable = new DataDefinitionQuery(
                databaseName,
                tableName,
                Set.of(new FieldImpl("id", FieldType.LONG)),
                Set.of(new IndexImpl(List.of("id"), IndexType.PRIMARY)),
                DataDefinitionQuery.Type.CREATE_TABLE
        );

        ResultQuery resultQuery = dataDefinitionQueryResolver.createTable(dataDefinitionQueryCreateTable);

        fileManager.loadTableScheme(databaseName, databaseContext.databases().stream().
                findFirst()
                .orElseThrow()
                .tables()
                .get(0)
                .name()
        );
        Assert.assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }
}