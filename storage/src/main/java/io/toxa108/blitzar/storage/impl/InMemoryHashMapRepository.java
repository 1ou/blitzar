package io.toxa108.blitzar.storage.impl;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void removeAll() {
        map.clear();
    }
}
