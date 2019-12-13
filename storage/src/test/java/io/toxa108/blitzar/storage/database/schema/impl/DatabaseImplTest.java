package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseImplTest {

    @Test
    public void initialize_db_when_success() {
        final Database database = new DatabaseImpl("testdb");
        ResultQuery resultQuery = database.initializeDatabase();
        assertEquals(EmptySuccessResultQuery.class, resultQuery.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_db_when_error_in_name() {
        final Database database = new DatabaseImpl("test_db");
    }

    @Test
    public void create_db_when_success() {
        final Database database = new DatabaseImpl("testdb");
        assertNotNull(database);
    }
}