package io.toxa108.blitzar.storage.query;

import io.toxa108.blitzar.storage.query.impl.DataManipulationQuery;

public interface DataManipulationQueryResolver {
    /**
     * Insert data in table
     *
     * @param query query
     * @return result
     */
    ResultQuery insert(DataManipulationQuery query);

    /**
     * Update data in table
     *
     * @param query query
     * @return result
     */
    ResultQuery update(DataManipulationQuery query);

    /**
     * Delete from table
     *
     * @param query query
     * @return result
     */
    ResultQuery delete(DataManipulationQuery query);

    /**
     * Select from table
     *
     * @param query query
     * @return result
     */
    ResultQuery select(DataManipulationQuery query);
}
