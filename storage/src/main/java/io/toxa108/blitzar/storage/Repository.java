package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.inmemory.entity.Result;

import java.util.List;
import java.util.Optional;

public interface Repository<K, T> {
    T add(K key, T value);
    List<Result<K, T>> all();
    Optional<T> findByKey(K key);
    void removeAll();
}
