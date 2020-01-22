package io.toxa108.blitzar.storage.query;

public interface QueryProcessor {
    /**
     * Transform input bytes to the query
     *
     * @param userContext user context
     * @param request     bytes
     * @return result byte array
     */
    byte[] process(UserContext userContext, byte[] request);
}
