package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.schema.*;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

public class DiskTreeManagerTest {

    private final BytesManipulator bytesManipulator = new BytesManipulatorImpl();

    @Test
    public void add_element_when_success() {
        FileManager fileManager = new TestFileManagerImpl(
                "/tmp/blitzar", new DatabaseConfigurationImpl(16));
        Database database = fileManager.initializeDatabase("test");

        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[0]);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[0]);
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

    @Test
    public void save_non_leaf_node_when_success() throws IOException {
        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[100]);
        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(1);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        int n = 10;
        Key[] keys = new Key[n];
        int[] p = new int[n + 1];
        for (int i = 0; i < n; ++i) {
            keys[i] = new KeyImpl(new FieldImpl(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, bytesManipulator.longToBytes(i + 1)));
            p[i] = -1;
        }
        p[n] = -1;

        DiskTreeManager.TreeNode treeNode = new DiskTreeManager.TreeNode(keys, p, false, n, -1);
        diskTreeManager.saveNode(databaseConfiguration.metadataSize() + 1, treeNode);
        DiskTreeManager.TreeNode treeNode1 =
                diskTreeManager.loadNode(databaseConfiguration.metadataSize() + 1);

        Assert.assertEquals(treeNode, treeNode1);
    }
}