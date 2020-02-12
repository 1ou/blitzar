package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.context.impl.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseImplTest {
    private final FileManager fileManager = new FileManagerImpl("/tmp/blitzar",
            new DatabaseConfigurationImpl(16));

    public DatabaseImplTest() throws IOException {
    }

    @BeforeEach
    public void before() {
        fileManager.clear();
    }

    @Test
    public void initialize_db_when_success() throws IOException {
        final Database database = fileManager.initializeDatabase("testdb");
        assertEquals("testdb", database.name());
    }

    @Test
    public void create_db_when_error_name_not_specified() throws IOException {
        assertThrows(NullPointerException.class, () -> fileManager.initializeDatabase(null));
    }

    @Test
    public void create_db_when_error_in_name() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> fileManager.initializeDatabase("test_%db"));
    }

    @Test
    public void create_db_when_success() throws IOException {
        final Database database = new BzDatabase("testdb", fileManager);
        assertNotNull(database);
    }
}