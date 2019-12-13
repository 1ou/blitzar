package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Table;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TableImplTest {
    @Test(expected = NullPointerException.class)
    public void create_table_when_error_name_not_specified() {
        final Table table = new TableImpl(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_table_when_error_in_name() {
        final Table table = new TableImpl("test_table");
    }

    @Test
    public void create_table_when_success() {
        final Table table = new TableImpl("table");
        assertNotNull(table);
    }
}