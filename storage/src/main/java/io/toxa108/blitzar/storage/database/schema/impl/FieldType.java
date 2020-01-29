package io.toxa108.blitzar.storage.database.schema.impl;

import java.util.NoSuchElementException;

public enum FieldType {
    /**
     * 1 byte
     */
    SHORT((short) 1, false, 2),
    /**
     * 4 bytes
     */
    INTEGER((short) 2, false, 4),
    /**
     * 8 bytes
     */
    LONG((short) 3, false, 8),
    /**
     * length * size(char)
     */
    VARCHAR((short) 4, false, 255);

    /**
     * TRUE if the size of one record can be different from another one
     */
    private final short id;
    private final boolean variable;
    private final int size;

    FieldType(final short id, final boolean variable, final int size) {
        this.id = id;
        this.variable = variable;
        this.size = size;
    }

    public boolean isVariable() {
        return variable;
    }

    public short id() {
        return id;
    }

    public static FieldType fromId(final short id) {
        for (FieldType e : values()) {
            if (e.id == id) {
                return e;
            }
        }
        throw new NoSuchElementException("Such field type doesn't exist");
    }

    public int size() {
        return size;
    }
}
