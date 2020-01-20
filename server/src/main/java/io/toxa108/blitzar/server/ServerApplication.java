package io.toxa108.blitzar.server;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.toxa108.blitzar.storage.BlitzarDatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;

public class ServerApplication {
    private static BlitzarDatabase database;
    private static Integer port;

    public static void main(String[] args) throws IOException, InterruptedException {
        database = new BlitzarDatabase("/tmp/blitzarprod");

        port = args.length > 0 ? Integer.parseInt(args[0]) : 9009;
        Server server = NettyServerBuilder.forPort(port)
                .addService(new SqlServiceImpl(database))
                .addService(new HealthCheckingServiceImpl())
                .addService(ProtoReflectionService.newInstance())
                .build();

        Runnable runnable = ServerApplication::dialog;

        server.start();
        Executors.newSingleThreadExecutor().submit(runnable);

        registerInConsul();
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
            System.out.println(new String(database.queryProcessor().process(null, command.getBytes())));
        }
    }

    private static void registerInConsul() {
        Consul client = Consul.builder().build();
        AgentClient agentClient = client.agentClient();

        String serviceId = UUID.randomUUID().toString();
        Registration service = ImmutableRegistration.builder()
                .id(serviceId)
                .name("blitzar")
                .check(Registration.RegCheck.grpc("localhost:9009", 10L))
                .tags(Collections.singletonList("tag1"))
                .meta(Collections.singletonMap("version", "1.0"))
                .build();

        agentClient.register(service);
    }
}
