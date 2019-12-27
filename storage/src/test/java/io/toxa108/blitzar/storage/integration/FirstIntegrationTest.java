package io.toxa108.blitzar.storage.integration;

import io.toxa108.blitzar.storage.BlitzarDatabase;
import org.junit.Assert;
import org.junit.Test;

public class FirstIntegrationTest {
    private final BlitzarDatabase blitzarDatabase = new BlitzarDatabase("/tmp/blitzar");

    /**
     * Test create database, table, insert row, select row through API.
     */
    @Test
    public void test() {
        String ddlDatabaseResult =
                new String(blitzarDatabase.queryProcessor().process("create database test;".getBytes()));
        Assert.assertEquals("success", ddlDatabaseResult);

        String changeDatabaseResult =
                new String(blitzarDatabase.queryProcessor().process("use test;".getBytes()));
        Assert.assertEquals("success", changeDatabaseResult);

        String ddlTableResult =
                new String(blitzarDatabase.queryProcessor().process("create table money;".getBytes()));
        Assert.assertEquals("success", ddlTableResult);

    }
}
