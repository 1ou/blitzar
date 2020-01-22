package io.toxa108.blitzar.storage.inmemory;

import io.toxa108.blitzar.storage.inmemory.entity.Result;

import java.util.List;
import java.util.Optional;

public interface Repository<K, V> {
    /**
     * Add key and value to collection
     *
     * @param key   key
     * @param value value
     * @return saved value
     */
    V add(K key, V value);

    /**
     * Get all collection elements
     *
     * @return all elements
     */
    List<Result<K, V>> all();

    /**
     * Find value by key
     * @param key key
     * @return value
     */
    Optional<V> findByKey(K key);

    /**
     * Remove all elements
     */
    void removeAll();
}
