package io.toxa108.blitzar.storage.database.manager;

public class ArrayManipulator {
    public <T> void insertInArray(T[] array, T value, int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    public void insertInArray(int[] array, int value, int pos) {
        if (array.length - 1 - pos >= 0) {
            System.arraycopy(array, pos, array, pos + 1, array.length - 1 - pos);
        }
        array[pos] = value;
    }

    public <T> void copyArray(T[] source, T[] destination, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    public void copyArray(int[] source, int[] destination, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, 0, destination, 0, len);
        }
    }

    public <T> void copyArray(T[] source, T[] destination, int pos, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }

    public void copyArray(int[] source, int[] destination, int pos, int len) {
        if (source.length < len || destination.length < len) {
            throw new IllegalArgumentException();
        }

        if (len >= 0) {
            System.arraycopy(source, pos, destination, 0, len);
        }
    }
}
