package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseImplTest {
    private final FileManager fileManager = new FileManagerImpl();

    @Test
    public void initialize_db_when_success() {
        final Database database = fileManager.initializeDatabase("testdb");
        assertEquals("testdb", database.name());
    }

    @Test(expected = NullPointerException.class)
    public void create_db_when_error_name_not_specified() {
        final Database database  = fileManager.initializeDatabase(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_db_when_error_in_name() {
        final Database database  = fileManager.initializeDatabase("test_db");
    }

    @Test
    public void create_db_when_success() {
        final Database database = new DatabaseImpl("testdb", fileManager);
        assertNotNull(database);
    }
}