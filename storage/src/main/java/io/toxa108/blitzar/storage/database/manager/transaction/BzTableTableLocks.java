package io.toxa108.blitzar.storage.database.manager.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A shared (S) lock permits the transaction that holds the lock to read a row.
 * An exclusive (X) lock permits the transaction that holds the lock to update or delete a row.
 */
public class BzTableTableLocks implements TableLocks {

    private static class SemaphoreReentrantReadWriteLock {
        private ReentrantReadWriteLock lock;
        private AtomicInteger numberReadLocks = new AtomicInteger(0);

        public SemaphoreReentrantReadWriteLock() {
            this.lock = new ReentrantReadWriteLock(true);
        }
    }

    private final ConcurrentHashMap<String, SemaphoreReentrantReadWriteLock> map =
            new ConcurrentHashMap<>(1024);

    @Override
    public void shared(int x) {
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> new SemaphoreReentrantReadWriteLock()
        );
        if (lock.lock.isWriteLocked()) {
            lock.lock.writeLock().lock();
        }
        lock.lock.readLock().lock();
        lock.numberReadLocks.incrementAndGet();
    }

    /**
     * https://stackoverflow.com/questions/464784/java-reentrantreadwritelocks-how-to-safely-acquire-write-lock
     * https://github.com/npgall/concurrent-locks
     *
     * there is no possibility to upgrade ReentrantReadWriteLock from read lock to write! you got deadlock.
     * @param x  position
     */
    @Override
    public void exclusive(int x) {
        System.out.println(Thread.currentThread().getName() + "  111111");
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> {
                    System.out.println("created new");
                    return new SemaphoreReentrantReadWriteLock();
                }
        );
        if (!lock.lock.isWriteLocked()) {
            System.out.println(Thread.currentThread().getName() + "  3333");
        }
        lock.lock.writeLock().lock();
        System.out.println(Thread.currentThread().getName() + "  22222");
    }

    @Override
    public void unshared(int x) {
        SemaphoreReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock.lock != null) {
            /*
                If no one has monitor of position {@param x} then remove read lock
             */
            if (lock.numberReadLocks.decrementAndGet() == 0) {
                lock.lock.readLock().unlock();
            }
        }
    }

    @Override
    public void unexclusive(int x) {
        SemaphoreReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock.lock != null) {
            lock.lock.writeLock().unlock();
        }
    }
}
