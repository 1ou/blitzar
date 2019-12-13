package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.query.Query;
import io.toxa108.blitzar.storage.query.ResultQuery;

public interface DatabaseManager {
    ResultQuery resolveQuery(Query query);
}
