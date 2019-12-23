package io.toxa108.blitzar.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.toxa108.blitzar.service.proto.SqlRequest;
import io.toxa108.blitzar.service.proto.SqlResponse;
import io.toxa108.blitzar.service.proto.SqlServiceGrpc;

public class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    @Override
    public void request(SqlRequest request, StreamObserver<SqlResponse> responseObserver) {
        byte[] bytes = {0, 1, 2, 3};
        SqlResponse sqlResponse = SqlResponse.newBuilder()
                .setAnswer(ByteString.copyFrom(bytes))
                .build();

        responseObserver.onNext(sqlResponse);
        responseObserver.onCompleted();
    }
}
