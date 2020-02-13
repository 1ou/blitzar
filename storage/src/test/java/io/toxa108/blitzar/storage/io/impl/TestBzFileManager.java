package io.toxa108.blitzar.storage.io.impl;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.context.impl.BzDatabaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestBzFileManager extends BzFileManager {
    public TestBzFileManager(String baseFolder, DatabaseConfiguration databaseConfiguration) throws IOException {
        super(baseFolder, databaseConfiguration);
    }

    public TestBzFileManager(String baseFolder) throws IOException {
        super(baseFolder, new BzDatabaseConfiguration(16));
    }

    @Override
    protected File createDirectory(String directory, String folderName) {
        try {
            File file = Files.createTempDirectory(Path.of(directory), folderName).toFile();
            file.deleteOnExit();
            return file;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected File createFile(String directory, String fileName, String fileExtension) {
        try {
            File file = Files.createTempFile(
                    Path.of(directory),
                    fileName, "." + fileExtension
            ).toFile();
            file.deleteOnExit();
            return file;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
