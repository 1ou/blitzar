package io.toxa108.blitzar.storage.query.command;

public interface SqlCommandFactory {
    /**
     * Initialize command
     * @return sql command
     */

    SqlCommand initializeCommand(String[] parts);
}
