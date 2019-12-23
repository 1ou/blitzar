package io.toxa108.blitzar.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

public class ServerApplication {
    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = NettyServerBuilder.forPort(9906)
                .addService(new SqlServiceImpl())
                .build();

        server.start();
        server.awaitTermination();
    }
}
