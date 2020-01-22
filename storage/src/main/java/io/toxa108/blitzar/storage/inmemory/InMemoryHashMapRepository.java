package io.toxa108.blitzar.storage.inmemory;

import io.toxa108.blitzar.storage.inmemory.entity.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * In hash map implementation.
 * @param <K>
 * @param <V>
 */
public class InMemoryHashMapRepository<K, V> implements Repository<K, V> {
    private final Map<K, V> map = new HashMap<>();

    @Override
    public V add(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public List<Result<K, V>> all() {
        return map.entrySet()
                .stream()
                .map(e -> new Result<>(e.getKey(), e.getValue()))
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Optional<V> findByKey(K key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void removeAll() {
        map.clear();
    }
}
