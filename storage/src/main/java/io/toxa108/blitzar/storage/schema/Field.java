package io.toxa108.blitzar.storage.schema;

import io.toxa108.blitzar.storage.schema.impl.FieldType;

public interface Field {
    String name();
    FieldType type();
    byte[] value();
}
