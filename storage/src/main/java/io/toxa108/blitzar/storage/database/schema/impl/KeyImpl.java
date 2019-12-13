package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Key;

public class KeyImpl implements Key {
    private final Field field;
    private final KeyType keyType;

    public KeyImpl(Field field, KeyType keyType) {
        this.field = field;
        this.keyType = keyType;
    }
}
