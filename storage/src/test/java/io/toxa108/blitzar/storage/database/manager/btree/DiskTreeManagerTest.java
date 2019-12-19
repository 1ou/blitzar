package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import org.junit.Test;

import java.util.Set;

public class DiskTreeManagerTest {

    @Test
    public void add_element_when_success() {
        FileManager fileManager = new TestFileManagerImpl("/tmp/blitzar");
        Database database = fileManager.initializeDatabase("test");

        Field fieldId = new FieldImpl("id", FieldType.LONG);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, 10);
        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        Table table = database.createTable("table", scheme);

        table.addRow(new RowImpl(
                new KeyImpl(fieldId),
                Set.of(fieldName)
        ));
    }
}