package io.toxa108.blitzar.storage.database.manager;

public interface LockManager {
    /**
     * Shared lock file position
     *
     * @param id unique id
     * @param x  position
     */
    void shared(String id, int x);

    /**
     * Exclusive lock file position
     *
     * @param id unique id
     * @param x  position
     */
    void exclusive(String id, int x);

    /**
     * Unlock file position
     *
     * @param id unique id
     * @param x  position
     */
    void unlock(String id, int x);
}
