package io.toxa108.blitzar.storage.database.schema.impl;

public enum IndexType {
    /**
     * Primary index - always clustered
     */
    PRIMARY((short) 1),

    /**
     * Secondary index
     */
    SECONDARY((short) 2);

    private final short id;

    private IndexType(short id) {
        this.id = id;
    }

    public short id() {
        return id;
    }

    public static IndexType fromId(short id) {
        return PRIMARY;
    }
}
