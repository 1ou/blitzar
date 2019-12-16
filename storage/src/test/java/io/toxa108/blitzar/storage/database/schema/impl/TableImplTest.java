package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TableImplTest {
    FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");

    @Test
    public void create_table_when_success() {
        final Table table = new TableImpl("table", fileManager);
        assertNotNull(table);
    }
}