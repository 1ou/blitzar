package io.toxa108.blitzar.storage.database.schema.impl;

/**
 * @author toxa
 */

public enum FieldType {
    /**
     * 1 byte
     */
    SHORT(false),
    /**
     * 4 bytes
     */
    INTEGER(false),
    /**
     * 8 bytes
     */
    LONG(false),
    /**
     * length * size(char)
     */
    VARCHAR(false);

    private final boolean variable;

    private FieldType(boolean variable) {
        this.variable = variable;
    }

    public boolean isVariable() {
        return variable;
    }
}
