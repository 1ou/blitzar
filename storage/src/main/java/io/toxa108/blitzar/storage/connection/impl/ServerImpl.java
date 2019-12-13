package io.toxa108.blitzar.storage.connection.impl;

import io.toxa108.blitzar.storage.connection.Server;

public class ServerImpl implements Server {
    private final int port;

    public ServerImpl(int port) {
        this.port = port;
    }
}
