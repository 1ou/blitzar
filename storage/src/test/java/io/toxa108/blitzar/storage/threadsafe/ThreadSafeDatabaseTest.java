package io.toxa108.blitzar.storage.threadsafe;

import io.toxa108.blitzar.storage.BzDatabase;
import io.toxa108.blitzar.storage.database.manager.user.BzUser;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.BzUserContext;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThreadSafeDatabaseTest {
    private final Logger log = LoggerFactory.getLogger(ThreadSafeDatabaseTest.class);
    private BzDatabase bzDatabase;
    private UserContext userContext;

    public ThreadSafeDatabaseTest() {
        bzDatabase = new BzDatabase("/tmp/blitzar");
        bzDatabase.clear();
        userContext = new BzUserContext(
                new BzUser(
                        "admin",
                        "123"
                )
        );
        bzDatabase.queryProcessor().process(userContext, "create database test;".getBytes());
        bzDatabase.queryProcessor().process(userContext, "use test;".getBytes());
        bzDatabase.queryProcessor().process(userContext, "create table example (time long not null primary key, value long not null);".getBytes());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 5, 7, 10})
    public void fill_database_Ok(int threads) throws ExecutionException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(threads);
        final ExecutorService service = Executors.newFixedThreadPool(threads);
        final List<String> keys = new CopyOnWriteArrayList<>();
        final Collection<Future<String>> futures = new ArrayList<>(threads);

        for (int t = 0; t < threads; ++t) {
            futures.add(service.submit(() -> {
                final String nanoTime = String.valueOf(System.nanoTime());
                keys.add(nanoTime);
                final String query = String.format(
                        "insert into example (time, value) values (%s, %s);", nanoTime, nanoTime);
                log.info("Thread: " + Thread.currentThread().getName() + "Query: " + query);

                final String result = new String(bzDatabase.queryProcessor().process(userContext, query.getBytes()));
                log.info("Thread: " + Thread.currentThread().getName() + "Result: " + result);

                countDownLatch.countDown();
                return result;
            }));
        }

        for (Future<String> f : futures) {
            final String r = f.get();
            log.info(new String(new EmptySuccessResultQuery().toBytes()) + " == " + r);
        }

        countDownLatch.await();
        final String all = new String(bzDatabase.queryProcessor().process(
                userContext,
                "select * from example;".getBytes()
        ));
        assertEquals(threads, all.chars().filter(it -> it == '\n').count());
    }
}