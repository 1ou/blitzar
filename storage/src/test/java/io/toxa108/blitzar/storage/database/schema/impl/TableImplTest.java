package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.impl.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.manager.row.RowManagerImpl;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class TableImplTest {
    DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(16);
    FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar", databaseConfiguration);

    public TableImplTest() throws IOException {
    }

    @Before
    public void before() {
        fileManager.clear();
    }

    @Test
    public void create_table_when_success() throws IOException {
        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();

        Scheme scheme = new SchemeImpl(Set.of(
                new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES])),
                Set.of(new IndexImpl(Set.of("id"), IndexType.PRIMARY)));

        final Table table = new TableImpl(
                "table",
                scheme,
                new RowManagerImpl(file, scheme, new DatabaseConfigurationImpl(1))
        );
        assertNotNull(table);
    }
}