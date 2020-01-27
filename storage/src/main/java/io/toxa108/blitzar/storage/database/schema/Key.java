package io.toxa108.blitzar.storage.database.schema;

public interface Key extends Comparable<Key> {
    /**
     * Fields of the key
     *
     * @return field
     */
    Field field();
}
