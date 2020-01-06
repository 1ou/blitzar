package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.FileManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class FileManagerImplTest {
    DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(16);

    @Before
    public void before() throws IOException {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar", databaseConfiguration);
        fileManager.clear();
    }

    @Test
    public void save_table_metadata_to_the_table_file_when_success() throws IOException {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar", databaseConfiguration);
        Database database = fileManager.initializeDatabase("test");
        Scheme scheme = new SchemeImpl(
                Set.of(new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[0]),
                        new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[10])
                ),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY),
                        new IndexImpl(Set.of("id", "name"), IndexType.SECONDARY)
                )
        );

        Table table = database.createTable("table", scheme);
        Table loadedTable = fileManager.loadTable(database.name(), table.name());
        Assert.assertEquals(scheme, loadedTable.scheme());
    }
}