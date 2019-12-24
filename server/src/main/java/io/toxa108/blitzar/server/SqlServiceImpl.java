package io.toxa108.blitzar.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.toxa108.blitzar.service.proto.SqlRequest;
import io.toxa108.blitzar.service.proto.SqlResponse;
import io.toxa108.blitzar.service.proto.SqlServiceGrpc;
import io.toxa108.blitzar.storage.BlitzarDatabase;

public class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    private final BlitzarDatabase database;

    public SqlServiceImpl(BlitzarDatabase blitzarDatabase) {
        this.database = blitzarDatabase;
    }

    @Override
    public void request(SqlRequest request, StreamObserver<SqlResponse> responseObserver) {
        database.queryProcessor().process(request.getSql().toByteArray());

        byte[] bytes = {0, 1, 2, 3};
        SqlResponse sqlResponse = SqlResponse.newBuilder()
                .setAnswer(ByteString.copyFrom(bytes))
                .build();

        responseObserver.onNext(sqlResponse);
        responseObserver.onCompleted();
    }
}
