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
     * Unlock file position
     *
     * @param x  position
     */
    void unlock(int x);
}
