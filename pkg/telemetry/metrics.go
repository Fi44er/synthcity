package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	meter = otel.Meter("synthcity-common")

	RequestCounter, _  = meter.Int64Counter("requests_total")
	RequestDuration, _ = meter.Float64Histogram("requests_duration", metric.WithUnit("s"))
	ActiveRequests, _  = meter.Int64UpDownCounter("requests_active")
)

func RecordMetrics(ctx context.Context, start time.Time, service, method, status string) {
	elapsed := time.Since(start).Seconds()
	attrs := metric.WithAttributes(
		attribute.String("service_name", service),
		attribute.String("method", method),
		attribute.String("status", status),
	)

	RequestCounter.Add(ctx, 1, attrs)
	RequestDuration.Record(ctx, elapsed, attrs)
}

func MetricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	serviceName := "hi-service"

	ActiveRequests.Add(ctx, 1, metric.WithAttributes(attribute.String("service_name", serviceName)))
	defer ActiveRequests.Add(ctx, -1, metric.WithAttributes(attribute.String("service_name", serviceName)))

	resp, err := handler(ctx, req) // Выполняем сам метод (например, SayHi)

	statusStr := grpcStatusToHTTP(err)

	RecordMetrics(ctx, start, serviceName, info.FullMethod, statusStr)

	return resp, err
}

func grpcStatusToHTTP(err error) string {
	if err == nil {
		return "200" // Статус OK
	}

	st, ok := status.FromError(err)
	if !ok {
		return "500" // Неизвестная ошибка
	}

	switch st.Code() {
	case codes.OK:
		return "200"
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated:
		return "400"
	case codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return "503"
	case codes.Internal, codes.DataLoss, codes.Unimplemented:
		return "500"
	default:
		return "500"
	}
}
