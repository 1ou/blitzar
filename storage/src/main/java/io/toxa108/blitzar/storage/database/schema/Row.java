package io.toxa108.blitzar.storage.database.schema;

import java.util.Set;

public interface Row {
    /**
     * Return fields in a row
     *
     * @return set of fields
     */
    Set<Field> fields();
}
