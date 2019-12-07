import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.impl.InMemoryTreeRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.Comparator;

@State(Scope.Benchmark)
public class InMemoryTreeRepositoryBenchmark {

    private Repository<Long, Long> repository = new InMemoryTreeRepository<>(
            Comparator.comparingLong((k) -> k));

    @Benchmark
    public void test() {

    }
}
