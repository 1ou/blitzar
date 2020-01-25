package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.database.schema.impl.KeyImpl;

public interface Key extends Comparable<Key> {
    /**
     * Fields of the key
     *
     * @return field
     */
    Field field();

    /*
     todo i need more time to decide is whether it bad practice or not
     */

    /**
     * Create new instance of {@link Key} from {@link Field}
     *
     * @param field field
     * @return key instance
     */
    static Key fromField(Field field) {
        return new KeyImpl(field);
    }
}
