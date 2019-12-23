package io.toxa108.blitzar.storage.database.manager.btree;

import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.manager.ArrayManipulator;
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
    public void save_and_load_non_leaf_node_when_success() throws IOException {
        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[100]);

        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("t2", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(1);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        int n = 48;
        Key[] keys = new Key[n];
        int[] p = new int[n + 1];
        for (int i = 0; i < n; ++i) {
            keys[i] = new KeyImpl(new FieldImpl(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, bytesManipulator.longToBytes(i + 1)));
            p[i] = -1;
        }
        p[n] = -1;

        int pos = databaseConfiguration.metadataSize();
        DiskTreeManager.TreeNode treeNode = new DiskTreeManager.TreeNode(pos, keys, p, false, n, -1);
        diskTreeManager.saveNode(pos, treeNode);
        DiskTreeManager.TreeNode treeNode1 =
                diskTreeManager.loadNode(pos);

        Assert.assertEquals(treeNode, treeNode1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void save_and_load_non_leaf_node_when_error() throws IOException {
        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[100]);

        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("t2", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(1);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        int n = 62;
        Key[] keys = new Key[n];
        int[] p = new int[n + 1];
        for (int i = 0; i < n; ++i) {
            keys[i] = new KeyImpl(new FieldImpl(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, bytesManipulator.longToBytes(i + 1)));
            p[i] = -1;
        }
        p[n] = -1;

        int pos = databaseConfiguration.metadataSize();
        DiskTreeManager.TreeNode treeNode = new DiskTreeManager.TreeNode(pos, keys, p, false, n, -1);
        diskTreeManager.saveNode(pos, treeNode);
        DiskTreeManager.TreeNode treeNode1 =
                diskTreeManager.loadNode(pos);

        Assert.assertEquals(treeNode, treeNode1);
    }

    @Test
    public void save_and_load_leaf_node_when_success() throws IOException {
        int nameLen = 100;
        int catLen = 2;

        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new FieldImpl("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[nameLen]);
        Field fieldCategory = new FieldImpl("category", FieldType.SHORT, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[catLen]);

        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        int n = 10;
        Key[] keys = new Key[n];

        byte[][] bytes = new byte[n][scheme.recordSize()];
        for (int i = 0; i < n; ++i) {
            keys[i] = new KeyImpl(new FieldImpl(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, bytesManipulator.longToBytes(i + 1)));

            String name = "example" + (i + 1) + "%";
            byte[] valueBytes = new byte[nameLen + catLen];
            System.arraycopy(name.getBytes(), 0, valueBytes, 0, name.length());

            short cat = (short) (i + 10);
            byte[] catBytes = bytesManipulator.shortToBytes(cat);

            System.arraycopy(catBytes, 0, valueBytes, nameLen, catLen);
            bytes[i] = valueBytes;
        }

        int pos = databaseConfiguration.metadataSize() + 1;
        DiskTreeManager.TreeNode treeNode = new DiskTreeManager.TreeNode(pos, keys, bytes, true, n, -1);
        diskTreeManager.saveNode(pos, treeNode);
        DiskTreeManager.TreeNode treeNode1 =
                diskTreeManager.loadNode(pos);

        Assert.assertEquals(treeNode, treeNode1);
    }

    @Test
    public void add_row_when_success() throws IOException {
        int nameLen = 100;
        int catLen = 2;

        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);

        String name = "exampleeeeeee" + "%";
        byte[] nameBytes = new byte[nameLen];
        System.arraycopy(name.getBytes(), 0, nameBytes, 0, name.length());

        Field fieldName = new FieldImpl(
                "name",
                FieldType.VARCHAR,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                nameBytes
        );
        Field fieldCategory = new FieldImpl(
                "category",
                FieldType.SHORT,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                bytesManipulator.shortToBytes((short) 99)
        );

        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        Key key = new KeyImpl(fieldId);
        Row row = new RowImpl(key, Set.of(fieldId, fieldName, fieldCategory));

        diskTreeManager.addRow(row);
    }

    @Test
    public void insert_in_array_test() {
        ArrayManipulator arrayManipulator = new ArrayManipulator();
        Integer[] arr = new Integer[10];

        for (int i = 0; i < 9; ++i) arr[i] = i;

        arrayManipulator.insertInArray(arr, 66, 4);
        Assert.assertArrayEquals(new Integer[] {0, 1, 2, 3, 66, 4, 5, 6, 7, 8}, arr);
    }

    @Test
    public void copy_array_test() {
        ArrayManipulator arrayManipulator = new ArrayManipulator();

        Integer[] arr = new Integer[10];
        for (int i = 0; i < 10; ++i) arr[i] = i;

        Integer[] res = new Integer[4];
        arrayManipulator.copyArray(arr, res, 4, 4);
        Assert.assertArrayEquals(new Integer[] {4, 5, 6, 7}, res);
    }

    @Test
    public void add_rows_when_success() throws IOException {
        int nameLen = 100;
        int catLen = 2;

        Field fieldId = new FieldImpl("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new FieldImpl(
                "name",
                FieldType.VARCHAR,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                new byte[nameLen]
        );
        Field fieldCategory = new FieldImpl(
                "category",
                FieldType.SHORT,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                new byte[catLen]
        );

        Scheme scheme = new SchemeImpl(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new IndexImpl(Set.of("id"), IndexType.PRIMARY)
                )
        );

        File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        for (int i = 0; i < 1000; ++i) {
            fieldId = new FieldImpl("id", FieldType.LONG,
                    Nullable.NOT_NULL, Unique.UNIQUE, bytesManipulator.longToBytes(i + 1));

            String name = "justname" + (i + 1) + "%";
            byte[] nameBytes = new byte[nameLen];
            System.arraycopy(name.getBytes(), 0, nameBytes, 0, name.length());

            fieldName = new FieldImpl(
                    "name",
                    FieldType.VARCHAR,
                    Nullable.NOT_NULL,
                    Unique.NOT_UNIQUE,
                    nameBytes
            );
            fieldCategory = new FieldImpl(
                    "category",
                    FieldType.SHORT,
                    Nullable.NOT_NULL,
                    Unique.NOT_UNIQUE,
                    bytesManipulator.shortToBytes((short) (i + 1))
            );

            Key key = new KeyImpl(fieldId);
            Row row = new RowImpl(key, Set.of(fieldId, fieldName, fieldCategory));
            diskTreeManager.addRow(row);
        }

        for (int i = 0; i < 1000; ++i) {
            fieldId = new FieldImpl("id", FieldType.LONG,
                    Nullable.NOT_NULL, Unique.UNIQUE, bytesManipulator.longToBytes(i + 1));
            Key key = new KeyImpl(fieldId);
            Row row = diskTreeManager.search(key);
            String compareStr = "justname" + (i + 1) + "%";
            Assert.assertEquals(row.key(), key);
            Assert.assertEquals(compareStr, new String(row.fieldByName("name").value()).substring(0, compareStr.length()));
        }
    }
}