package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.NotNull;

public class ArrayManipulator {
    public <T> void insertInArray(@NotNull T[] array, @NotNull T value, @NotNull int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    public void insertInArray(@NotNull int[] array, @NotNull int value, @NotNull int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    public <T> void copyArray(@NotNull T[] source, @NotNull T[] destination, @NotNull int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    public void copyArray(@NotNull int[] source, @NotNull int[] destination, @NotNull int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    public <T> void copyArray(@NotNull T[] source, @NotNull T[] destination, @NotNull int pos, @NotNull int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }

    public void copyArray(@NotNull int[] source, @NotNull int[] destination, @NotNull int pos, @NotNull int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }
}
