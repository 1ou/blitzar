package io.toxa108.blitzar.storage.database.schema.impl;

import java.util.NoSuchElementException;

public enum FieldType {
    /**
     * 1 byte
     */
    SHORT((short) 1, false),
    /**
     * 4 bytes
     */
    INTEGER((short) 2, false),
    /**
     * 8 bytes
     */
    LONG((short) 3, false),
    /**
     * length * size(char)
     */
    VARCHAR((short) 4, false);

    /**
     * TRUE if the size of one record can be different from another one
     */
    private final short id;
    private final boolean variable;

    FieldType(short id, boolean variable) {
        this.id = id;
        this.variable = variable;
    }

    public boolean isVariable() {
        return variable;
    }

    public short id() {
        return id;
    }

    public static FieldType fromId(short id) {
        for (FieldType e : values()) {
            if (e.id == id) {
                return e;
            }
        }
        throw new NoSuchElementException("Such field type doesn't exist");
    }
}
