package io.toxa108.blitzar.server;

import grpc.health.v1.HealthCheckingService;
import grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;

public class HealthCheckingServiceImpl extends HealthGrpc.HealthImplBase {
    @Override
    public void check(HealthCheckingService.HealthCheckRequest request, StreamObserver<HealthCheckingService.HealthCheckResponse> responseObserver) {
        responseObserver.onNext(HealthCheckingService.HealthCheckResponse.newBuilder()
                .setStatus(HealthCheckingService.HealthCheckResponse.ServingStatus.SERVING)
                .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void watch(HealthCheckingService.HealthCheckRequest request, StreamObserver<HealthCheckingService.HealthCheckResponse> responseObserver) {
        responseObserver.onNext(HealthCheckingService.HealthCheckResponse.newBuilder()
                .setStatus(HealthCheckingService.HealthCheckResponse.ServingStatus.SERVING)
                .build()
        );
        responseObserver.onCompleted();
    }
}
