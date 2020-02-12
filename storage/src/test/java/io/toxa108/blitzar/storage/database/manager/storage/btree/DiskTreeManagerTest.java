package io.toxa108.blitzar.storage.database.manager.storage.btree;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.impl.DatabaseConfigurationImpl;
import io.toxa108.blitzar.storage.database.manager.storage.btree.impl.*;
import io.toxa108.blitzar.storage.database.schema.*;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.FileManager;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.TestFileManagerImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DiskTreeManagerTest {

    @Test
    public void add_element_when_success() throws IOException {
        FileManager fileManager = new TestFileManagerImpl(
                "/tmp/blitzar", new DatabaseConfigurationImpl(16));
        Database database = fileManager.initializeDatabase("test");

        Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[0]);
        Field fieldName = new BzField("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[0]);
        Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        Table table = database.createTable("table", scheme);

        table.addRow(new BzRow(
                new BzKey(fieldId),
                Set.of(fieldName)
        ));
    }
    @Test
    public void save_and_load_non_leaf_node_when_success() throws IOException {
        final Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        final Field fieldName = new BzField("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[100]);

        final Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("t2", "12").toFile();
        file.deleteOnExit();
        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(1);
        final TableBTreeMetadata tableBTreeMetadata = new TableBTreeMetadataImpl(file, databaseConfiguration, scheme);
        final DiskBTreeWriter diskBTreeWriter = new DiskBTreeWriterImpl(file, tableBTreeMetadata);
        final DiskBTreeReader diskBTreeReader = new DiskBTreeReaderImpl(file, tableBTreeMetadata);

        final int n = 48;
        final Key[] keys = new Key[n];
        final int[] p = new int[n + 1];
        for (int i = 0; i < n; ++i) {
            keys[i] = new BzKey(new BzField(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, BytesManipulator.longToBytes(i + 1)));
            p[i] = -1;
        }
        p[n] = -1;

        final int pos = databaseConfiguration.metadataSize();
        final TreeNode treeNode = new TreeNode(pos, keys, p, false, n, -1);
        diskBTreeWriter.write(pos, treeNode);
        final TreeNode treeNode1 = diskBTreeReader.read(pos);

        assertEquals(treeNode, treeNode1);
    }

    @Test
    public void save_and_load_non_leaf_node_when_error() throws IOException {
        final Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        final Field fieldName = new BzField("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[100]);

        final Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("t2", "12").toFile();
        file.deleteOnExit();
        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(1);
        final TableBTreeMetadata tableBTreeMetadata = new TableBTreeMetadataImpl(file, databaseConfiguration, scheme);
        final DiskBTreeWriter diskBTreeWriter = new DiskBTreeWriterImpl(file, tableBTreeMetadata);

        int n = 62;
        Key[] keys = new Key[n];
        int[] p = new int[n + 1];
        for (int i = 0; i < n; ++i) {
            keys[i] = new BzKey(new BzField(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, BytesManipulator.longToBytes(i + 1)));
            p[i] = -1;
        }
        p[n] = -1;

        final int pos = databaseConfiguration.metadataSize();
        final TreeNode treeNode = new TreeNode(pos, keys, p, false, n, -1);
        assertThrows(IllegalArgumentException.class, () -> diskBTreeWriter.write(pos, treeNode));
    }

    @Test
    public void save_and_load_leaf_node_when_success() throws IOException {
        final int nameLen = 100;
        final int catLen = 2;

        final Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        final Field fieldName = new BzField("name", FieldType.VARCHAR, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[nameLen]);
        final Field fieldCategory = new BzField("category", FieldType.SHORT, Nullable.NOT_NULL, Unique.NOT_UNIQUE, new byte[catLen]);

        final Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        final TableBTreeMetadata tableBTreeMetadata = new TableBTreeMetadataImpl(file, databaseConfiguration, scheme);
        final DiskBTreeWriter diskBTreeWriter = new DiskBTreeWriterImpl(file, tableBTreeMetadata);
        final DiskBTreeReader diskBTreeReader = new DiskBTreeReaderImpl(file, tableBTreeMetadata);

        final int n = 14;
        Key[] keys = new Key[n];

        final byte[][] bytes = new byte[n][scheme.recordSize()];
        for (int i = 0; i < n; ++i) {
            keys[i] = new BzKey(new BzField(
                    "id", FieldType.LONG, Nullable.NOT_NULL,
                    Unique.UNIQUE, BytesManipulator.longToBytes(i + 1)));

            final String name = "example" + (i + 1) + "%";
            final byte[] valueBytes = new byte[nameLen + catLen];
            System.arraycopy(name.getBytes(), 0, valueBytes, 0, name.length());

            final short cat = (short) (i + 10);
            final byte[] catBytes = BytesManipulator.shortToBytes(cat);

            System.arraycopy(catBytes, 0, valueBytes, nameLen, catLen);
            bytes[i] = valueBytes;
        }

        final int pos = databaseConfiguration.metadataSize() + 1;
        final TreeNode treeNode = new TreeNode(pos, keys, bytes, true, n, -1);
        diskBTreeWriter.write(pos, treeNode);
        final TreeNode treeNode1 = diskBTreeReader.read(pos);

        assertEquals(treeNode, treeNode1);
    }

    @Test
    public void add_row_when_success() throws IOException {
        final int nameLen = 100;

        final Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);

        String name = "exampleeeeeee" + "%";
        final byte[] nameBytes = new byte[nameLen];
        System.arraycopy(name.getBytes(), 0, nameBytes, 0, name.length());

        final Field fieldName = new BzField(
                "name",
                FieldType.VARCHAR,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                nameBytes
        );
        final Field fieldCategory = new BzField(
                "category",
                FieldType.SHORT,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                BytesManipulator.shortToBytes((short) 99)
        );

        final Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        final DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        final Key key = new BzKey(fieldId);
        final Row row = new BzRow(key, Set.of(fieldId, fieldName, fieldCategory));

        diskTreeManager.addRow(row);
    }

    @Test
    public void add_rows_when_success_case_1() throws IOException {
        final Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        final Scheme scheme = new BzScheme(
                Set.of(fieldId),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();

        final DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration() {
            @Override
            public int metadataSize() {
                return 1024;
            }

            @Override
            public int diskPageSize() {
                return 120;
            }
        };

        final DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        final long[] keys = {5, 8, 1, 7, 3};
        for (long k : keys) {
            Field newFieldId = new BzField(
                    "id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, BytesManipulator.longToBytes(k));

            Key key = new BzKey(newFieldId);
            Row row = new BzRow(key, Set.of(newFieldId));
            diskTreeManager.addRow(row);
        }
    }

    @Test
    public void add_rows_when_success_case_2() throws IOException {
        final int nameLen = 100;
        final int catLen = 2;

        Field fieldId = new BzField("id", FieldType.LONG, Nullable.NOT_NULL, Unique.UNIQUE, new byte[Long.BYTES]);
        Field fieldName = new BzField(
                "name",
                FieldType.VARCHAR,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                new byte[nameLen]
        );
        Field fieldCategory = new BzField(
                "category",
                FieldType.SHORT,
                Nullable.NOT_NULL,
                Unique.NOT_UNIQUE,
                new byte[catLen]
        );

        final Scheme scheme = new BzScheme(
                Set.of(fieldId, fieldName, fieldCategory),
                Set.of(
                        new BzIndex(Set.of("id"), IndexType.PRIMARY)
                )
        );

        final File file = Files.createTempFile("q1", "12").toFile();
        file.deleteOnExit();
        final DatabaseConfiguration databaseConfiguration = new DatabaseConfigurationImpl(2);
        final DiskTreeManager diskTreeManager = new DiskTreeManager(
                file,
                databaseConfiguration,
                scheme
        );

        for (int i = 0; i < 1000; ++i) {
            fieldId = new BzField("id", FieldType.LONG,
                    Nullable.NOT_NULL, Unique.UNIQUE, BytesManipulator.longToBytes(i + 1));

            final String name = "justname" + (i + 1) + "%";
            byte[] nameBytes = new byte[nameLen];
            System.arraycopy(name.getBytes(), 0, nameBytes, 0, name.length());

            fieldName = new BzField(
                    "name",
                    FieldType.VARCHAR,
                    Nullable.NOT_NULL,
                    Unique.NOT_UNIQUE,
                    nameBytes
            );
            fieldCategory = new BzField(
                    "category",
                    FieldType.SHORT,
                    Nullable.NOT_NULL,
                    Unique.NOT_UNIQUE,
                    BytesManipulator.shortToBytes((short) (i + 1))
            );

            final Key key = new BzKey(fieldId);
            final Row row = new BzRow(key, Set.of(fieldId, fieldName, fieldCategory));
            diskTreeManager.addRow(row);
        }

        for (int i = 0; i < 1000; ++i) {
            fieldId = new BzField("id", FieldType.LONG,
                    Nullable.NOT_NULL, Unique.UNIQUE, BytesManipulator.longToBytes(i + 1));
            final Key key = new BzKey(fieldId);
            final List<Row> rows = diskTreeManager.search(key);
            final String compareStr = "justname" + (i + 1) + "%";
            assertEquals(rows.get(0).key(), key);
            assertEquals(compareStr, new String(rows.get(0).fieldByName("name").value()).substring(0, compareStr.length()));
        }
    }
}