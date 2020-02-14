package io.toxa108.blitzar.storage.database.manager.row;

import io.toxa108.blitzar.storage.database.context.DatabaseConfiguration;
import io.toxa108.blitzar.storage.database.manager.storage.btree.impl.BzTreeTables;
import io.toxa108.blitzar.storage.database.schema.Field;
import io.toxa108.blitzar.storage.database.schema.Row;
import io.toxa108.blitzar.storage.database.schema.Scheme;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class BzRows implements Rows {
    private final BzTreeTables bzTreeTables;

    public BzRows(final File file,
                  final Scheme scheme,
                  final DatabaseConfiguration databaseConfiguration) {
        this.bzTreeTables = new BzTreeTables(file, databaseConfiguration, scheme);
    }

    @Override
    public void add(final Row row) {
        try {
            bzTreeTables.addRow(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Row> search(Field field) {
        try {
            return bzTreeTables.search(field);
        } catch (IOException e) {
            return null; // todo
        }
    }

    @Override
    public List<Row> search() {
        try {
            return bzTreeTables.search();
        } catch (IOException e) {
            return null; // todo
        }
    }
}
