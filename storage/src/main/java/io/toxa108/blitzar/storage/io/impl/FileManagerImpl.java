package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.NotNull;
import io.toxa108.blitzar.storage.database.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.RowManagerImpl;
import io.toxa108.blitzar.storage.database.schema.*;
import io.toxa108.blitzar.storage.database.schema.impl.*;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.DiskWriter;
import io.toxa108.blitzar.storage.io.FileManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

public class FileManagerImpl implements FileManager {
    protected final String baseFolder;
    private final String nameRegex = "^[a-zA-Z0-9_]*$";
    private final String tableExtension = "ddd";
    private final BytesManipulator bytesManipulator;
    private final int m = 1024;
    private final DatabaseConfiguration databaseConfiguration;

    public FileManagerImpl(@NotNull final String baseFolder,
                           @NotNull final DatabaseConfiguration databaseConfiguration) {
        this.baseFolder = baseFolder;
        this.bytesManipulator = new BytesManipulatorImpl();
        this.databaseConfiguration = databaseConfiguration;
    }

    @Override
    public List<String> databases() {
        File[] folders = new File(baseFolder).listFiles(File::isDirectory);
        if (folders != null) {
            return Arrays.stream(folders)
                    .map(File::getName)
                    .collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public Database initializeDatabase(@NotNull final String name) throws IOException {
        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect database name");
        }

        File file = createDirectory(baseFolder, name);
        return new DatabaseImpl(file.getName(), this);
    }

    /**
     * initialize table: create file on disk
     *
     * @param databaseName database
     * @param tableName    name
     * @param scheme       scheme
     * @return table
     * @throws IllegalArgumentException when the name of database of table isn't correct
     */
    @Override
    public Table initializeTable(@NotNull final String databaseName,
                                 @NotNull final String tableName,
                                 @NotNull final Scheme scheme) throws IOException {
        if (!databaseName.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect database name");
        }

        if (!tableName.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect table name");
        }

        File file = createFile(baseFolder + "/" + databaseName, tableName, this.tableExtension);
        this.saveTableScheme(file, scheme);
        return new TableImpl(
                file.getName(),
                scheme,
                new RowManagerImpl(file, scheme, databaseConfiguration)
        );
    }

    /**
     * Table stores in one file. The page_size by default equals 16 kilobytes.
     * | **** |
     * | Meta information of table = page_size
     * | page_size * 1024 - 2048 = offset index data (bytes)
     * | indexes data = | number of indexes 2 bytes | arr of indexes size = 2 * number of indexes bytes | data
     * | page_size * 1024 - 1024 = offset fields data (bytes)
     * | fields data = | number of fields 2 bytes | arr of fields size = 2 * number of fields bytes | data
     * | **** |
     * | Records data
     * | B (block size) = page_zie * 1024
     * | R (record size) - size of record in bytes
     * | B >= R
     * | bfr - blocking factor B / R (rounds down) - number of records in file
     * | Unused space in block (bytes) = B - (bfr * R)
     * | **** |
     *
     * @param file   file
     * @param scheme table scheme
     * @throws IOException disk io exception
     */
    private void saveTableScheme(@NotNull final File file,
                                 @NotNull final Scheme scheme) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        accessFile.setLength(databaseConfiguration.diskPageSize() * 20);
        DiskWriter diskWriter = new DiskWriterIoImpl(file);

        int posOfIndexes = databaseConfiguration.diskPageSize() - m * 2;
        int startOfIndexes = posOfIndexes;
        diskWriter.write(posOfIndexes, bytesManipulator.intToBytes(scheme.indexes().size()));
        int tmpSeek = posOfIndexes;
        posOfIndexes += Integer.BYTES * scheme.indexes().size() + Integer.BYTES;

        for (Index index : scheme.indexes()) {
            byte[] bytes = index.toBytes();
            tmpSeek += Integer.BYTES;
            diskWriter.write(tmpSeek, bytesManipulator.intToBytes(posOfIndexes - startOfIndexes));
            diskWriter.write(posOfIndexes, bytes);
            posOfIndexes += bytes.length;
        }

        int posOfFields = databaseConfiguration.diskPageSize() - m;
        startOfIndexes = posOfFields;
        diskWriter.write(posOfFields, bytesManipulator.intToBytes(scheme.fields().size()));
        tmpSeek = posOfFields;
        posOfFields += Integer.BYTES * scheme.fields().size() + Integer.BYTES;

        for (Field field : scheme.fields()) {
            byte[] bytes = field.metadataToBytes();
            tmpSeek += Integer.BYTES;
            diskWriter.write(tmpSeek, bytesManipulator.intToBytes(posOfFields - startOfIndexes));
            diskWriter.write(posOfFields, bytes);
            posOfFields += bytes.length;
        }
    }

    private Scheme loadTableScheme(@NotNull final File file) throws IOException {
        DiskReader diskReader = new DiskReaderIoImpl(file);
        Set<Index> indexes = new HashSet<>();

        int posOfIndexes = databaseConfiguration.diskPageSize() - m * 2;
        int startOfIndexes = posOfIndexes;

        int sizeOfIndexes = bytesManipulator.bytesToInt(diskReader.read(posOfIndexes, Integer.BYTES));
        for (int i = 0; i < sizeOfIndexes; ++i) {
            posOfIndexes += Integer.BYTES;
            int seekOfIndex = bytesManipulator.bytesToInt(
                    diskReader.read(posOfIndexes, Integer.BYTES));

            int indexSize = bytesManipulator.bytesToInt(
                    diskReader.read(startOfIndexes + seekOfIndex, Integer.BYTES));

            byte[] bytes = diskReader.read(startOfIndexes + seekOfIndex, indexSize + Integer.BYTES);
            Index index = new IndexImpl(bytes);
            indexes.add(index);
        }

        Set<Field> fields = new HashSet<>();
        int posOfFields = databaseConfiguration.diskPageSize() - m;
        int startOfFields = posOfFields;

        int sizeOfFields = bytesManipulator.bytesToInt(diskReader.read(posOfFields, Integer.BYTES));
        for (int i = 0; i < sizeOfFields; ++i) {
            posOfFields += Integer.BYTES;
            int seekOfField = bytesManipulator.bytesToInt(
                    diskReader.read(posOfFields, Integer.BYTES));

            int fieldSize = bytesManipulator.bytesToInt(
                    diskReader.read(startOfFields + seekOfField, Integer.BYTES));

            byte[] bytes = diskReader.read(startOfFields + seekOfField, fieldSize + Integer.BYTES);
            Field field = new FieldImpl(bytes);
            fields.add(field);
        }

        return new SchemeImpl(fields, indexes);
    }

    @Override
    public List<Table> loadTables(@NotNull final String databaseName) throws IOException {
        File[] files = new File(baseFolder + "/" + databaseName)
                .listFiles(File::isFile);

        if (files != null) {
            List<Table> tables = new ArrayList<>();
            for (File file : files) {

                Scheme scheme = this.loadTableScheme(file);
                tables.add(new TableImpl(
                        file.getName(),
                        scheme,
                        new RowManagerImpl(file, scheme, databaseConfiguration)
                ));
            }
            return tables;
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public Table loadTable(@NotNull final String databaseName,
                           @NotNull final String tableName) throws IOException {
        File[] files = new File(baseFolder + "/" + databaseName).listFiles(File::isFile);
        if (files != null) {
            List<Table> tables = new ArrayList<>();
            for (File file : files) {
                if (file.getName().equals(tableName)) {
                    Scheme scheme = this.loadTableScheme(file);
                    tables.add(
                            new TableImpl(
                                    file.getName(),
                                    scheme,
                                    new RowManagerImpl(file, scheme, databaseConfiguration)
                            )
                    );
                }
            }

            if (tables.isEmpty()) {
                throw new NoSuchElementException(
                        String.format("Table %s doesn't found on disk", tableName));
            } else {
                return tables.get(0);
            }
        } else {
            throw new NoSuchElementException(String.format("Table %s doesn't found on disk", tableName));
        }
    }

    @Override
    public void clear() {
        File[] files = new File(baseFolder).listFiles();
        if (files != null) {
            for (File file : files) {
                clear(file);
                file.delete();
            }
        }
    }

    private void clear(@NotNull final File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File file1 : files) {
                    clear(file1);
                }
            }
        } else {
            file.delete();
        }
    }

    /**
     * Create directory
     *
     * @param directory directoryName
     * @return path
     * @throws IllegalStateException disk io error
     */
    protected File createDirectory(@NotNull final String directory,
                                   @NotNull final String folderName) {
        File newDirectory = new File(directory + "/" + folderName);
        if (newDirectory.mkdir() || newDirectory.exists()) {
            return newDirectory;
        } else {
            throw new IllegalStateException("Can't create file");
        }
    }

    protected File createFile(@NotNull final String directory,
                              @NotNull final String fileName,
                              @NotNull final String fileExtension) {
        return new File(
                new File(directory),
                fileName + "." + fileExtension
        );
    }
}
