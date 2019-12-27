package io.toxa108.blitzar.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.toxa108.blitzar.service.proto.*;
import io.toxa108.blitzar.storage.BlitzarDatabase;
import io.toxa108.blitzar.storage.NotNull;

public class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    private final BlitzarDatabase database;

    public SqlServiceImpl(@NotNull final BlitzarDatabase blitzarDatabase) {
        this.database = blitzarDatabase;
    }

    @Override
    public void request(@NotNull final SqlRequest request,
                        @NotNull final StreamObserver<SqlResponse> responseObserver) {
        database.queryProcessor().process(request.getSql().toByteArray());

        byte[] bytes = {0, 1, 2, 3};
        SqlResponse sqlResponse = SqlResponse.newBuilder()
                .setAnswer(ByteString.copyFrom(bytes))
                .build();

        responseObserver.onNext(sqlResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void auth(@NotNull final AuthRequest request,
                     @NotNull final StreamObserver<AuthResponse> responseObserver) {
        AuthResponse authResponse = AuthResponse.newBuilder()
                .setStatus(AuthResponse.Status.ACCEPTED)
                .build();

        responseObserver.onNext(authResponse);
        responseObserver.onCompleted();
    }
}
