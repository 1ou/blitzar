package io.toxa108.blitzar.storage.database.manager.transaction;

public interface TableLocks {
    /**
     * Shared lock file position
     *
     * @param x position
     */
    void shared(int x);

    /**
     * If exclusive lock
     *
     * @param x position
     */
    boolean isExclusive(int x);

    /**
     * Exclusive lock file position
     *
     * @param x position
     */
    void exclusive(int x);

    /**
     * Unlock read
     *
     * @param x position
     */
    void unshared(int x);

    /**
     * Unlock exclusive
     *
     * @param x position
     */
    void unexclusive(int x);
}
