package io.toxa108.blitzar.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.toxa108.blitzar.storage.BlitzarDatabase;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executors;

public class ServerApplication {
    private static BlitzarDatabase database;

    public static void main(String[] args) throws IOException, InterruptedException {
        database = new BlitzarDatabase();

        Server server = NettyServerBuilder.forPort(9906)
                .addService(new SqlServiceImpl(database))
                .build();

        Runnable runnable = ServerApplication::dialog;

        server.start();
        Executors.newSingleThreadExecutor().submit(runnable);
        server.awaitTermination();
    }

    private static void dialog() {
        Scanner scanner =new Scanner(System.in);
        System.out.println("Time series blitzar database.");

        while (true) {
            String command = scanner.next();
            System.out.println(new String(database.queryProcessor().process(command.getBytes())));
            if ("exit".equals(command)) {
                break;
            }
        }
    }
}
