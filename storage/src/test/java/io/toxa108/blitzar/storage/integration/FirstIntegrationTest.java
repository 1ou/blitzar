package io.toxa108.blitzar.storage.integration;

import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.database.manager.user.User;
import io.toxa108.blitzar.storage.database.manager.user.UserImpl;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.UserContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FirstIntegrationTest {
    private final BlitzarDatabase blitzarDatabase = new BlitzarDatabase("/tmp/blitzar");
    private User user;

    @BeforeEach
    public void init() {
        blitzarDatabase.clear();
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
        assertEquals("success", ddlDatabaseResult);


        userContext = new UserContextImpl(
                "test", new UserImpl("toxa", "123321"));

        String ddlTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "create table example ( time long not null primary key , value long not null );".getBytes())
                );

        assertEquals("success", ddlTableResult);

        String insertTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "insert into example ( time , value ) values ( 30000 , 200 );".getBytes())
                );

        assertEquals("success", insertTableResult);

        insertTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "insert into example ( time , value ) values ( 30001 , 201 );".getBytes())
                );

        assertEquals("success", insertTableResult);

        String selectFromTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "select * from example;".getBytes())
                );

        assertEquals("value 200\nvalue 201\n", selectFromTableResult);

        String selectFromWhereTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "select * from example where time = 30000;".getBytes())
                );

        assertEquals("value 200\n", selectFromWhereTableResult);

        selectFromWhereTableResult =
                new String(blitzarDatabase.queryProcessor().process(
                        userContext,
                        "select * from example where value = 201;".getBytes())
                );

        assertEquals("value 201\n", selectFromWhereTableResult);
    }
}
