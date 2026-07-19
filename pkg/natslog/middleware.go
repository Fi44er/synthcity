package natslog

import (
	"context"

	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

func Middleware(serviceName string) func(next func(ctx context.Context, msg *nats.Msg)) func(*nats.Msg) {
	return func(next func(ctx context.Context, msg *nats.Msg)) func(*nats.Msg) {
		return func(msg *nats.Msg) {
			ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header))

			telemetry.ActiveRequests.Add(ctx, 1, metric.WithAttributes(attribute.String("service_name", serviceName)))
			tracer := otel.Tracer(serviceName)
			ctx, span := tracer.Start(ctx, "nats.subscribe: "+msg.Subject)
			defer span.End()

			next(ctx, msg)
		}
	}
}

// InjectTraceHeader помогает при отправке сообщения
func InjectTraceHeader(ctx context.Context, msg *nats.Msg) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
}
