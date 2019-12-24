package io.toxa108.blitzar.storage.query;

public interface QueryProcessor {
    /**
     * transform input bytes to the query
     * @param request bytes
     * @return result byte array
     */
    byte[] process(byte[] request);
}
