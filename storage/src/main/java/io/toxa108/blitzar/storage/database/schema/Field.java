package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.FieldType;

public interface Field {
    String name();
    FieldType type();
    byte[] value();
}
