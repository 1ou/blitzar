package io.toxa108.blitzar.storage.database.manager.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A shared (S) lock permits the transaction that holds the lock to read a row.
 * An exclusive (X) lock permits the transaction that holds the lock to update or delete a row.
 */
public class BzTableTableLocks implements TableLocks {

    private static class SemaphoreReentrantReadWriteLock {
        private ReentrantLock readLock;
        private ReentrantLock writeLock;
        private AtomicInteger numberReadLocks = new AtomicInteger(0);

        public SemaphoreReentrantReadWriteLock() {
            this.readLock = new ReentrantLock(true);
            this.writeLock = new ReentrantLock(true);
        }
    }

    private final ConcurrentHashMap<String, SemaphoreReentrantReadWriteLock> map =
            new ConcurrentHashMap<>(1024);

    @Override
    public void shared(int x) {
        System.out.println(Thread.currentThread().getName() + " shared " + x);
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> new SemaphoreReentrantReadWriteLock()
        );
        if (lock.writeLock.isLocked()) {
            lock.writeLock.lock();
        }
        lock.readLock.lock();
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
    public synchronized void exclusive(int x) {
        System.out.println(Thread.currentThread().getName() + "  exclusive " + x);
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> new SemaphoreReentrantReadWriteLock()
        );
        lock.writeLock.lock();
    }

    @Override
    public void unshared(int x) {
        System.out.println(Thread.currentThread().getName() + " unshared " + x);
        final SemaphoreReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock != null) {
            /*
                If no one has monitor of position {@param x} then remove read lock
             */
            if (lock.numberReadLocks.decrementAndGet() == 0) {
                lock.readLock.unlock();
            }
        }
    }

    @Override
    public void unexclusive(int x) {
        System.out.println(Thread.currentThread().getName() + "  unexclusive " + x);
        SemaphoreReentrantReadWriteLock lock = map.get(String.valueOf(x));
        if (lock != null) {
            lock.writeLock.unlock();
        }
    }
}
