package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Field;

import java.util.Objects;

public class BzField implements Field {
    private final String name;
    private final FieldType fieldType;
    private final Nullable nullable;
    private final Unique unique;
    private final byte[] value;
    private final int valueSize;

    /**
     * Ctor.
     * @param name name
     * @param fieldType type
     * @param nullable nullable
     * @param unique unique
     * @param value value
     */
    public BzField(final String name,
                   final FieldType fieldType,
                   final Nullable nullable,
                   final Unique unique,
                   final byte[] value) {
        this.name = name;
        this.fieldType = fieldType;
        this.nullable = nullable;
        this.unique = unique;
        this.value = value;

        if (fieldType == FieldType.VARCHAR) {
            this.valueSize = value.length;
        } else {
            this.valueSize = fieldType.size();
        }
    }

    /**
     * Field name
     *
     * @return field name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Field type
     *
     * @return field type
     */
    @Override
    public FieldType type() {
        return fieldType;
    }

    /**
     * Filed value
     *
     * @return filed value
     */
    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public Nullable nullable() {
        return nullable;
    }

    @Override
    public Unique unique() {
        return unique;
    }

    @Override
    public int diskSize() {
        return valueSize;
    }

    @Override
    public boolean isVariable() {
        return fieldType.isVariable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BzField field = (BzField) o;
        return Objects.equals(name, field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
