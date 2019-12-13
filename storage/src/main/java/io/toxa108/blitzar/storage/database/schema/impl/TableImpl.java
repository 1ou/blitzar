package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;
import io.toxa108.blitzar.storage.query.ResultQuery;
import io.toxa108.blitzar.storage.query.impl.EmptySuccessResultQuery;

public class TableImpl implements Table {
    private final String name;
    private Scheme scheme;
    private final String nameRegex = "[a-zA-Z]+";

    public TableImpl(String name) {
        if (name == null) {
            throw new NullPointerException("The table name is not specified");
        }

        if (!name.matches(nameRegex)) {
            throw new IllegalArgumentException("Incorrect table name");
        }

        this.name = name;
    }

    @Override
    public ResultQuery initializeScheme(Scheme scheme) {
        this.scheme = scheme;
        return new EmptySuccessResultQuery();
    }

    @Override
    public String name() {
        return name;
    }
}
