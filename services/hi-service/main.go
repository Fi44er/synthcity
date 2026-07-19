package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/Fi44er/synthcity/api/gen/go/test/v1"
	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/natslog"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedHelloServiceServer
}

func (s *server) SayHi(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	l := logger.FromContext(ctx)

	l.Info("Handling gRPC SayHi", zap.String("user_name", in.GetName()))

	meter := otel.Meter("hi-service")
	counter, _ := meter.Int64Counter("greetings_total")
	counter.Add(ctx, 1)

	tr := otel.Tracer("hi-service")
	_, span := tr.Start(ctx, "db_query_user")
	time.Sleep(50 * time.Millisecond)
	span.End()

	l.Error("Test")

	return &pb.HelloResponse{Message: "Hi " + in.GetName() + " from gRPC Service!"}, nil
}

func main() {
	shutdown, _ := telemetry.Init("hi-service", "localhost:4318")
	defer shutdown()

	l := logger.New("hi-service", "info")
	defer l.Sync()

	nc, _ := nats.Connect("nats://city-nats:4222")
	mw := natslog.Middleware("hi-service")

	nc.Subscribe("greet.hello", mw(func(ctx context.Context, msg *nats.Msg) {
		log := logger.FromContext(ctx)
		log.Info("NATS message received", zap.ByteString("data", msg.Data))
	}))

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			telemetry.MetricsInterceptor,
			l.UnaryServerInterceptor(),
		),
	)

	pb.RegisterHelloServiceServer(s, &server{})

	l.Info("Hi-Service running", zap.String("grpc_port", "50051"), zap.String("nats_subject", "greet.hello"))

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
