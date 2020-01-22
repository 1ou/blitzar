package io.toxa108.blitzar.storage.database.schema.impl;

public enum KeyType {
    /**
     * Primary key (it's always a cluster key). One table can contain only one primary key.
     */
    PRIMARY,

    /**
     * Foreign key. One table can have several foreign keys. Using for merging in relational algebra.
     */
    FOREIGN
}
