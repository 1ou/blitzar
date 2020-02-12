package io.toxa108.blitzar.storage.threadsafe;

import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.database.manager.user.UserImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;
import io.toxa108.blitzar.storage.query.impl.UserContextImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThreadSafeDatabaseTest {
    private BlitzarDatabase blitzarDatabase;
    private UserContext userContext;

    public ThreadSafeDatabaseTest() {
        blitzarDatabase = new BlitzarDatabase("/tmp/blitzar");
        blitzarDatabase.clear();
        userContext = new UserContextImpl(
                new UserImpl(
                        "admin",
                        "123"
                )
        );
        blitzarDatabase.queryProcessor().process(userContext, "create database test;".getBytes());
        blitzarDatabase.queryProcessor().process(userContext, "use test;".getBytes());
        blitzarDatabase.queryProcessor().process(userContext, "create table example (time long not null primary key, value long not null);".getBytes());
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        int threads = 10;
        CountDownLatch countDownLatch = new CountDownLatch(threads);
        ExecutorService service = Executors.newFixedThreadPool(threads);
        List<String> keys = new CopyOnWriteArrayList<>();
        Collection<Future<String>> futures = new ArrayList<>(threads);

        for (int t = 0; t < threads; ++t) {
            futures.add(service.submit(() -> {
                String nanoTime = String.valueOf(System.nanoTime());
                keys.add(nanoTime);
                String query = String.format(
                        "insert into example (time, value) values (%s, %s);", nanoTime, nanoTime);
                System.out.println("Thread: " + Thread.currentThread().getName() + "Query: " + query);

                String result = new String(blitzarDatabase.queryProcessor().process(userContext, query.getBytes()));
                System.out.println("Thread: " + Thread.currentThread().getName() + "Result: " + result);

                countDownLatch.countDown();
                return result;
            }));
        }

        for (Future<String> f : futures) {
            String r = f.get();
            System.out.println(new String(new EmptySuccessResultQuery().toBytes()) + " == " + r);
        }

        countDownLatch.await();
        String all = new String(blitzarDatabase.queryProcessor().process(
                userContext,
                "select * from example;".getBytes()
        ));
        assertEquals(threads, all.chars().filter(it -> it == '\n').count());
    }
}