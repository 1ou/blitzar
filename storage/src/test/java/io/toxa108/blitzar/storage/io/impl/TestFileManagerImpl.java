package io.toxa108.blitzar.storage.io.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestFileManagerImpl extends FileManagerImpl {
    public TestFileManagerImpl(String baseFolder) {
        super(baseFolder);
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
