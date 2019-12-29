package io.toxa108.blitzar.storage.integration;

import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.database.manager.user.User;
import io.toxa108.blitzar.storage.database.manager.user.UserImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.UserContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FirstIntegrationTest {
    private final BlitzarDatabase blitzarDatabase = new BlitzarDatabase("/tmp/blitzar");
    private User user;

    @Before
    public void init() {
        user = blitzarDatabase.databaseManager().userManager().createUser("toxa", "123321");
    }

    /**
     * Test create database, table, insert row, select row through API.
     */
    @Test
    public void test() {
        UserContext userContext = new UserContextImpl(new UserImpl("toxa", "123321"));

        String ddlDatabaseResult =
                new String(blitzarDatabase.queryProcessor().process(userContext, "create database test;".getBytes()));
        Assert.assertEquals("success", ddlDatabaseResult);

        userContext = new UserContextImpl(
                "test", new UserImpl("toxa", "123321"));

        String ddlTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "create table money ( time long not null primary key, value long not null );".getBytes())
                );

        Assert.assertEquals("success", ddlTableResult);
    }
}
