package io.toxa108.blitzar.storage.database.manager.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A shared (S) lock permits the transaction that holds the lock to read a row.
 * An exclusive (X) lock permits the transaction that holds the lock to update or delete a row.
 */
public class BzTableTableLocks implements TableLocks {
    private final Logger log = LoggerFactory.getLogger(BzTableTableLocks.class);
    private final String tableName;

    /**
     * Ctor.
     *
     * @param tableName table name
     */
    public BzTableTableLocks(final String tableName) {
        this.tableName = tableName;
    }

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
    public void shared(final int x) {
        log.info("table" + tableName + " shared " + x);
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> new SemaphoreReentrantReadWriteLock()
        );
        lock.readLock.lock();
        lock.numberReadLocks.incrementAndGet();
        if (lock.writeLock.isLocked()) {
            lock.writeLock.lock();
        }
    }

    /**
     * https://stackoverflow.com/questions/464784/java-reentrantreadwritelocks-how-to-safely-acquire-write-lock
     * https://github.com/npgall/concurrent-locks
     * <p>
     * there is no possibility to upgrade ReentrantReadWriteLock from read lock to write! you got deadlock.
     *
     * @param x position
     */
    @Override
    public void exclusive(final int x) {
        log.info("table" + tableName + " exclusive " + x);
        final SemaphoreReentrantReadWriteLock lock = map.computeIfAbsent(
                String.valueOf(x),
                k -> new SemaphoreReentrantReadWriteLock()
        );
        lock.writeLock.lock();
    }

    @Override
    public void unshared(final int x) {
        log.info("table" + tableName + " unshared " + x);
        map.computeIfPresent(
                String.valueOf(x),
                (k, v) -> {
                    /*
                        If no one has monitor of position {@param x} then remove read lock
                    */
                    if (v.numberReadLocks.decrementAndGet() == 0) {
                        v.readLock.unlock();
                    }
                    return v;
                });
    }

    @Override
    public void unexclusive(final int x) {
        log.info("table" + tableName + " unexclusive " + x);
        map.computeIfPresent(
                String.valueOf(x),
                (k, v) -> {
                    v.writeLock.unlock();
                    return v;
                }
        );
    }

    @Override
    public int sharedCount(final int x) {
        return map.get(String.valueOf(x)).readLock.getHoldCount();
    }
}
