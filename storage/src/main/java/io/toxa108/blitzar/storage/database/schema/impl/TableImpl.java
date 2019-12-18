package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.io.FileManager;

import java.util.Objects;

public class TableImpl implements Table {
    private final String name;
    private final Scheme scheme;
    private final String nameRegex = "[a-zA-Z]+";
    private final FileManager fileManager;
    private final State state;

    public TableImpl(String name, Scheme scheme, FileManager fileManager) {
        this.fileManager = fileManager;
        this.name = name;
        this.state = State.EXISTS;
        this.scheme = scheme;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableImpl table = (TableImpl) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
