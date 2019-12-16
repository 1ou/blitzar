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
    private final String baseFolder = "blitzar";
    private final String nameRegex = "[a-zA-Z]+";

    public FileManagerImpl() {
        File newDirectory = new File(new File(System.getProperty("java.io.tmpdir")), baseFolder);
        if (!newDirectory.mkdir()) {
            newDirectory.mkdir();
        }
    }

    @Override
    public List<String> databases() {
        File[] folders = new File(System.getProperty("java.io.tmpdir") + "/" + baseFolder).listFiles(File::isDirectory);
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

        File newDirectory = new File(new File(System.getProperty("java.io.tmpdir") + "/" + baseFolder), name);
        if (newDirectory.mkdir() || newDirectory.exists()) {
            return new DatabaseImpl(name, this);
        } else {
            throw new IllegalArgumentException("Database can't be created");
        }
    }

    @Override
    public Table initializeTable(Database database, String name) {
        if (name == null) {
            throw new NullPointerException("The table name is not specified");
        }

        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect table name");
        }

        File databaseDirectory = new File(new File(
                System.getProperty("java.io.tmpdir") + "/" + baseFolder),
                name + ".ddd"
        );

        if (databaseDirectory.exists()) {
            return new TableImpl(name, this);
        } else {
            throw new IllegalArgumentException("Table can't be created");
        }
    }
}
