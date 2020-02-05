package io.toxa108.blitzar.storage.query.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseContext;
import io.toxa108.blitzar.storage.database.context.impl.DatabaseContextImpl;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import io.toxa108.blitzar.storage.query.DataDefinitionQueryResolver;
import io.toxa108.blitzar.storage.query.ResultQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DataDefinitionQueryResolverImplTest {
    @Before
    public void before() throws IOException {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");
        fileManager.clear();
    }

    @Test
    public void create_database_when_success() throws IOException {
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
    public void create_database_and_empty_table_when_success() throws IOException {
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

        Set<Field> fields = Set.of(new FieldImpl(
                "id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]));

        Set<Index> indexes = Set.of(new IndexImpl(Set.of("id"), IndexType.PRIMARY));

        DataDefinitionQuery dataDefinitionQueryCreateTable = new DataDefinitionQuery(
                databaseName, tableName, fields, indexes, DataDefinitionQuery.Type.CREATE_TABLE);

        ResultQuery resultQuery = dataDefinitionQueryResolver.createTable(dataDefinitionQueryCreateTable);

        Assert.assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }

    @Test
    public void create_database_and_table_with_scheme_when_success() throws IOException {
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
                Set.of(new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[8])),
                Set.of(new IndexImpl(Set.of("id"), IndexType.PRIMARY)),
                DataDefinitionQuery.Type.CREATE_TABLE
        );

        ResultQuery resultQuery = dataDefinitionQueryResolver.createTable(dataDefinitionQueryCreateTable);

        fileManager.loadTable(databaseName, databaseContext.databases().stream()
                .findFirst()
                .orElseThrow()
                .tables()
                .get(0)
                .name()
        );
        Assert.assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }
}