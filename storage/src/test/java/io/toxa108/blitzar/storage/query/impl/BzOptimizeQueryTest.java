package io.toxa108.blitzar.storage.query.impl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BzOptimizeQueryTest {
    private final BzOptimizeQuery bzOptimizeQuery = new BzOptimizeQuery();

    private static Stream<Arguments> test_parametersForOk() {
        return Stream.of(
                Arguments.of(
                        "create table database_name (time long not null primary key, value long not null);",
                        "create table database_name ( time long not null primary key , value long not null ) ;"
                ),
                Arguments.of(
                        "use      database_name  ;",
                        "use database_name ;"
                ),
                Arguments.of(
                        "select  *  from    database_name    ;",
                        "select * from database_name ;"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("test_parametersForOk")
    public void test_Ok(String before, String optimized) {
        assertEquals(optimized, bzOptimizeQuery.optimize(before));
    }
}