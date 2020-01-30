package io.toxa108.blitzar.storage.database.manager;

public interface LockManager {
    /**
     * Shared lock file position
     *
     * @param x  position
     */
    void shared(int x);

    /**
     * Exclusive lock file position
     *
     * @param x  position
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
