package io.toxa108.blitzar.server;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.database.manager.user.AccessDeniedException;
import io.toxa108.blitzar.storage.query.UserContext;
import io.toxa108.blitzar.storage.query.impl.UserContextImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;

public class ServerApplication {
    private static BlitzarDatabase database;
    private static Integer port;

    public static void main(String[] args) throws IOException, InterruptedException {
        database = new BlitzarDatabase("/tmp/blitzarprod");
        database.databaseManager().userManager().createUser("admin", "123");

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
        Optional<UserContext> userContext = Optional.empty();

        while (true) {
            if (userContext.isEmpty()) {
                System.out.println("Enter login & password:");

                String login, password;
                try {
                    login = ob.readLine();
                    password = ob.readLine();
                    userContext = Optional.of(
                            new UserContextImpl(
                                    database.databaseManager().userManager().authorize(login, password)
                            )
                    );
                } catch (IOException ignored) {
                } catch (AccessDeniedException e) {
                    System.out.println("Wrong credentials. Try again.");
                }
            } else {
                System.out.println("Enter a command:");
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
                System.out.println(new String(database.queryProcessor().process(
                        userContext.get(), command.getBytes())));
            }
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
