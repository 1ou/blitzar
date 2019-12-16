package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.FileManagerImpl;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TableImplTest {
    FileManager fileManager = new FileManagerImpl();

    @Test(expected = NullPointerException.class)
    public void create_table_when_error_name_not_specified() {
        final Table table = new TableImpl(null, fileManager);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_table_when_error_in_name() {
        final Table table = new TableImpl("test_table", fileManager);
    }

    @Test
    public void create_table_when_success() {
        final Table table = new TableImpl("table", fileManager);
        assertNotNull(table);
    }
}