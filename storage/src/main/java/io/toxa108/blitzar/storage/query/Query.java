package io.toxa108.blitzar.storage.query;

public interface Query {
    /**
     * Return database name
     * @return database name
     */
    String databaseName();

    /**
     * Return table name
     * @return table name
     */
    String tableName();
}
