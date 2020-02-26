package io.toxa108.blitzar.storage.database.manager.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertTrue;

class BzTableTableLocksTest {
    private TableLocks tableLocks;

    @BeforeEach
    public void init() {
        tableLocks = new BzTableTableLocks();
    }

    @RepeatedTest(value = 10)
    public void shared_lock_Ok() {
        final int threads = 10;
        final ExecutorService service = Executors.newFixedThreadPool(threads);
        final Collection<Future<?>> futures = new ArrayList<>(threads);

        for (int t = 0; t < threads; ++t) {
            futures.add(service.submit(() -> {
                tableLocks.shared(1);
            }));
        }
    }

    @RepeatedTest(value = 10)
    public void exclusive_Ok() throws InterruptedException {
        final Runnable r1 = () -> {
            try {
                tableLocks.exclusive(1);
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                tableLocks.unexclusive(1);
            }
        };

        final Runnable r2 = () -> {
            try {
                tableLocks.exclusive(1);
            } finally {
                tableLocks.unexclusive(1);
            }
        };

        final long start = System.currentTimeMillis();
        new Thread(r1).start();
        Thread.sleep(10);
        final Thread t2 = new Thread(r2);
        t2.start();
        t2.join();
        final long finish = System.currentTimeMillis();
        final long diff = finish - start;
        assertTrue(diff > 100);
    }

    @RepeatedTest(value = 10)
    public void exclusiveAndShared_Ok() throws InterruptedException {
        final Runnable r1 = () -> {
            try {
                tableLocks.exclusive(1);
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                tableLocks.unexclusive(1);
            }
        };

        final Runnable r2 = () -> {
            try {
                tableLocks.shared(1);
            } finally {
                tableLocks.unshared(1);
            }
        };

        final long start = System.currentTimeMillis();
        new Thread(r1).start();
        Thread.sleep(10);
        final Thread t2 = new Thread(r2);
        t2.start();
        t2.join();
        final long finish = System.currentTimeMillis();
        final long diff = finish - start;
        assertTrue(diff > 100);
    }
}