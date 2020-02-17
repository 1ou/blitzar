package io.toxa108.blitzar.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.toxa108.blitzar.service.proto.*;
import io.toxa108.blitzar.storage.BzDatabase;

public class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    private final BzDatabase database;

    public SqlServiceImpl(final BzDatabase bzDatabase) {
        this.database = bzDatabase;
    }

    @Override
    public void request(final SqlRequest request,
                        final StreamObserver<SqlResponse> responseObserver) {
        database.queryProcessor().process(null, request.getSql().toByteArray());

        byte[] bytes = {0, 1, 2, 3};
        SqlResponse sqlResponse = SqlResponse.newBuilder()
                .setAnswer(ByteString.copyFrom(bytes))
                .build();

        responseObserver.onNext(sqlResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void auth(final AuthRequest request,
                     final StreamObserver<AuthResponse> responseObserver) {
        AuthResponse authResponse = AuthResponse.newBuilder()
                .setStatus(AuthResponse.Status.ACCEPTED)
                .build();

        responseObserver.onNext(authResponse);
        responseObserver.onCompleted();
    }
}
