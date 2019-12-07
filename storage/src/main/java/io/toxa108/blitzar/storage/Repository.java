package io.toxa108.blitzar.storage;

import io.toxa108.blitzar.storage.entity.Result;

import java.util.List;

public interface Repository<K, T> {
    T add(K key, T value);
    List<Result<K, T>> all();
}
