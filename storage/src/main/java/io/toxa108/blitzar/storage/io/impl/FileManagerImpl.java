package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.database.schema.Database;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.database.schema.impl.DatabaseImpl;
import io.toxa108.blitzar.storage.database.schema.impl.TableImpl;
import io.toxa108.blitzar.storage.io.FileManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileManagerImpl implements FileManager {
    protected final String baseFolder;
    private final String nameRegex = "[a-zA-Z]+";
    private final String tableExtension = "ddd";

    public FileManagerImpl(String baseFolder) {
        this.baseFolder = baseFolder;
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
    public Database initializeDatabase(String name) {
        if (name == null) {
            throw new NullPointerException("The database name is not specified");
        }

        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect database name");
        }

        File file = createDirectory(baseFolder, name);
        return new DatabaseImpl(file.getName(), this);
    }

    @Override
    public Table initializeTable(Database database, String name) {
        if (name == null) {
            throw new NullPointerException("The table name is not specified");
        }

        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect table name");
        }

        File file = createFile(baseFolder + "/" + database.name(), name, this.tableExtension);
        return new TableImpl(file.getName(), this);
    }

    /**
     * Create directory
     * @param directory directoryName
     * @return path
     */
    protected File createDirectory(String directory, String folderName) {
        File newDirectory = new File(directory + "/" + folderName);
        if (newDirectory.mkdir() || newDirectory.exists()) {
            return newDirectory;
        } else {
            throw new IllegalStateException("Can't create file");
        }
    }

    protected File createFile(String directory, String fileName, String fileExtension) {
        return new File(
                new File(directory),
                fileName + "." + fileExtension
        );
    }
}
