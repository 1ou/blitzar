package io.toxa108.blitzar.storage.benchmark;

import io.toxa108.blitzar.storage.inmemory.InMemoryBPlusTreeRepository;
import io.toxa108.blitzar.storage.inmemory.InMemoryHashMapRepository;
import io.toxa108.blitzar.storage.inmemory.InMemoryTreeRepository;
import io.toxa108.blitzar.storage.inmemory.Repository;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class InMemoryRepositorySearchBenchmark {
    private Repository<Long, Long> inMemoryTreeRepository;
    private Repository<Long, Long> inMemoryHashMapRepository;
    private Repository<Long, Long> inMemoryBPlusTreeRepository;

    @Param({"10000"})
    private int N;

    @Setup
    public void setup() {
        inMemoryTreeRepository = new InMemoryTreeRepository<>(
                Comparator.comparingLong((k) -> k)
        );
        inMemoryHashMapRepository = new InMemoryHashMapRepository<>();
        inMemoryBPlusTreeRepository = new InMemoryBPlusTreeRepository<>(30, 29);

        for (long i = 0; i < N; ++i) {
            inMemoryTreeRepository.add(i, i);
            inMemoryHashMapRepository.add(i, i);
            inMemoryBPlusTreeRepository.add(i, i);
        }
    }

    @Benchmark
    public void test_tree_insert(Blackhole blackhole) {
        for (long i = 0; i < N; i++) {
            inMemoryTreeRepository.findByKey(i);
//            blackhole.consume(keys.get(i));
        }
    }

    @Benchmark
    public void test_hash_map_insert(Blackhole blackhole) {
        for (long i = 0; i < N; i++) {
            inMemoryHashMapRepository.findByKey(i);
//            blackhole.consume(keys.get(i));
        }
    }

    @Benchmark
    public void test_bplus_tree_insert(Blackhole blackhole) {
        for (long i = 0; i < N; i++) {
            inMemoryBPlusTreeRepository.findByKey(i);
//            blackhole.consume(keys.get(i));
        }
    }
}
