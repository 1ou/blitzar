package io.toxa108.blitzar.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.toxa108.blitzar.storage.BlitzarDatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;

public class ServerApplication {
    private static BlitzarDatabase database;

    public static void main(String[] args) throws IOException, InterruptedException {
        database = new BlitzarDatabase("/tmp/blitzarprod");

        Server server = NettyServerBuilder.forPort(9907)
                .addService(new SqlServiceImpl(database))
                .build();

        Runnable runnable = ServerApplication::dialog;

        server.start();
        Executors.newSingleThreadExecutor().submit(runnable);
        server.awaitTermination();
    }

    private static void dialog() {
        BufferedReader ob = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Time series database.");

        while (true) {
            System.out.println("Enter a string");
            String command;
            try {
                command = ob.readLine();
            } catch (IOException e) {
                command = "";
            }

            if ("exit".equals(command)) {
                break;
            }
            System.out.println();
            System.out.println(new String(database.queryProcessor().process(command.getBytes())));
        }
    }
}
