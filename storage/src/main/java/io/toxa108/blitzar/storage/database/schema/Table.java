package io.toxa108.blitzar.storage.database.schema;

import io.toxa108.blitzar.storage.query.ResultQuery;

public interface Table {
    /**
     * Initialize metadata for the scheme
     * @param scheme scheme
     */
    ResultQuery initializeScheme(Scheme scheme);

    /**
     * Name
     * @return name
     */
    String name();
}
