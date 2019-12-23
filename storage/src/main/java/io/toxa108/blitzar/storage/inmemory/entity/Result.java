package io.toxa108.blitzar.storage.inmemory.entity;

import lombok.Data;

@Data
public class Result<TIMESTAMP, DATA> {
    private final TIMESTAMP timestamp;
    private final DATA data;

    public Result(TIMESTAMP timestamp, DATA data) {
        this.timestamp = timestamp;
        this.data = data;
    }
}
