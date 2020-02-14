package io.toxa108.blitzar.storage.database.manager.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * A shared (S) lock permits the transaction that holds the lock to read a row.
 * An exclusive (X) lock permits the transaction that holds the lock to update or delete a row.
 *
 */
public class BzTableTableLocks implements TableLocks {
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> map =
            new ConcurrentHashMap<>(1024);

    @Override
    public void shared(int x) {
        ReentrantReadWriteLock lock = map.computeIfAbsent(String.valueOf(x), k -> new ReentrantReadWriteLock(true));
        if (lock.isWriteLocked()) {
            lock.writeLock().lock();
        }
        lock.readLock().lock();
    }

    @Override
    public void exclusive(int x) {
        ReentrantReadWriteLock lock = map.computeIfAbsent(String.valueOf(x), k -> new ReentrantReadWriteLock(true));
        lock.writeLock().lock();
    }

    @Override
    public void unshared(int x) {
        ReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock != null) {
            lock.readLock().unlock();
        }
    }

    @Override
    public void unexclusive(int x) {
        ReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock != null) {
            lock.writeLock().unlock();
        }
    }
}
