package io.toxa108.blitzar.storage.benchmark;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.impl.InMemoryHashMapRepository;
import io.toxa108.blitzar.storage.impl.InMemoryTreeRepository;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class InMemoryRepositoryInsertBenchmark {
    private Repository<Long, Long> inMemoryTreeRepository = new InMemoryTreeRepository<>(
            Comparator.comparingLong((k) -> k)
    );

    private Repository<Long, Long> inMemoryHashMapRepository = new InMemoryHashMapRepository<>();

    @Param({"10000000"})
    private int N;

    private List<Long> keys = new ArrayList<>();
    private List<Long> values = new ArrayList<>();

    @Setup
    public void setup() {
        for (int i = 0; i < N; ++i) {
            keys.add(System.nanoTime());
            values.add(ThreadLocalRandom.current().nextLong());
        }
    }

    @Benchmark
    public void test_tree_insert(Blackhole blackhole) {
        for (int i = 0; i < N; i++) {
            inMemoryTreeRepository.add(keys.get(i), values.get(i));
//            blackhole.consume(keys.get(i));
        }
    }


    @Benchmark
    public void test_hash_map_insert(Blackhole blackhole) {
        for (int i = 0; i < N; i++) {
            inMemoryHashMapRepository.add(keys.get(i), values.get(i));
//            blackhole.consume(keys.get(i));
        }
    }
}
