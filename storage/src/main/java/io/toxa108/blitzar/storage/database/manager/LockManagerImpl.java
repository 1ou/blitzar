package io.toxa108.blitzar.storage.database.manager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * A shared (S) lock permits the transaction that holds the lock to read a row.
 * An exclusive (X) lock permits the transaction that holds the lock to update or delete a row.
 *
 */
public class LockManagerImpl implements LockManager {
    private final ConcurrentHashMap<Integer, ReentrantLock> map =
            new ConcurrentHashMap<>(1024);

    @Override
    public void shared(int x) {

    }

    @Override
    public void exclusive(int x) {

    }

    @Override
    public void unlock(int x) {

    }
}
