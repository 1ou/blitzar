package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Scheme;
import io.toxa108.blitzar.storage.database.schema.Table;

public class TableImpl implements Table {
    private Scheme scheme;

    @Override
    public void initializeScheme(Scheme scheme) {
        this.scheme = scheme;
    }

    @Override
    public String name() {
        return null;
    }
}
