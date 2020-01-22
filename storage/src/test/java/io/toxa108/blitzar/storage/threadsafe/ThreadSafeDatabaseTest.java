package io.toxa108.blitzar.storage.threadsafe;

import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.database.manager.user.UserImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.UserContextImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        blitzarDatabase.queryProcessor().process(userContext, "create table example ( time long not null primary key , value long not null );".getBytes());
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        int threads = 10;
        ExecutorService service = Executors.newFixedThreadPool(threads);
        Collection<Future<byte[]>> futures = new ArrayList<>(threads);
        for (int t = 0; t < threads; ++t) {
            futures.add(service.submit(() -> {
                String nanoTime = String.valueOf(System.nanoTime());
                String query = String.format(
                        "insert into example ( time , value ) values ( %s , %s );", nanoTime, nanoTime);

                blitzarDatabase.queryProcessor().process(userContext, query.getBytes());
                return blitzarDatabase.queryProcessor().process(
                        userContext,
                        String.format("select * from example where time = %s;", nanoTime).getBytes()
                );
            }));
        }

        Set<String> results = new HashSet<>();
        for (Future<byte[]> f : futures) {
            results.add(new String(f.get()));
        }

        String all = new String(blitzarDatabase.queryProcessor().process(
                userContext,
                "select * from example;".getBytes()
        ));
        Assert.assertEquals(threads, all.chars().filter(it -> it == '\n').count());
    }
}