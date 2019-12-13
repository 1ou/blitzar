package io.toxa108.blitzar.storage.inmemory.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Result<TIMESTAMP, DATA> {
    private final TIMESTAMP timestamp;
    private final DATA data;
}
