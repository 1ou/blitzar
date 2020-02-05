package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.context.impl.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseImplTest {
    private final FileManager fileManager = new FileManagerImpl("/tmp/blitzar",
            new DatabaseConfigurationImpl(16));

    public DatabaseImplTest() throws IOException {
    }

    @Before
    public void before() {
        fileManager.clear();
    }

    @Test
    public void initialize_db_when_success() throws IOException {
        final Database database = fileManager.initializeDatabase("testdb");
        assertEquals("testdb", database.name());
    }

    @Test(expected = NullPointerException.class)
    public void create_db_when_error_name_not_specified() throws IOException {
        final Database database  = fileManager.initializeDatabase(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_db_when_error_in_name() throws IOException {
        final Database database  = fileManager.initializeDatabase("test_%db");
    }

    @Test
    public void create_db_when_success() throws IOException {
        final Database database = new DatabaseImpl("testdb", fileManager);
        assertNotNull(database);
    }
}