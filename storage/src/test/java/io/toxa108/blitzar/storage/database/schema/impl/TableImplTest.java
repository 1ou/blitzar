package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.manager.RowManagerImpl;
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

    @Before
    public void before() {
        fileManager.clear();
    }

    @Test
    public void create_table_when_success() throws IOException {
        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();

        final Table table = new TableImpl(
                "table",
                new SchemeImpl(Set.of(), Set.of()),
                new RowManagerImpl(file, null, null)
        );
        assertNotNull(table);
    }
}