package app

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/Fi44er/synthcity/services/map-service/internal/config"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/Fi44er/synthcity/services/map-service/internal/infrastructure/osm"
	service "github.com/Fi44er/synthcity/services/map-service/internal/service/route"
	transport "github.com/Fi44er/synthcity/services/map-service/internal/transport/grpc"
)

type App struct {
	cfg        *config.Config
	log        *logger.Logger
	grpcServer *grpc.Server
}

func New(cfg *config.Config) *App {
	l := logger.New("map-service", "info")

	var graph *domain.RoadGraph
	var err error
	binPath := cfg.PbfPath + ".bin"

	l.Info("Attempting to load binary graph cache", zap.String("path", binPath))
	graph, err = domain.LoadBinary(binPath)

	if err != nil {
		l.Warn("Binary cache not found or invalid, parsing PBF (this may take a while)", zap.Error(err))

		graph = domain.NewRoadGraph()
		graphProc := osm.NewGraphProcessor(graph)
		staticProc := osm.NewStaticProcessor()
		loader := osm.NewLoader(cfg.PbfPath, graphProc, staticProc)
		graph, err = loader.LoadGraph(context.Background())
		if err != nil {
			l.Fatal("Failed to load map from PBF", zap.Error(err))
		}

		if err := graph.SaveBinary(binPath); err != nil {
			l.Error("Failed to save binary cache", zap.Error(err))
		} else {
			l.Info("Binary cache saved for future fast boots")
		}
	} else {
		l.Info("Graph loaded from binary cache")
	}

	mapSvc := service.NewMapService(graph)

	handler := transport.NewHandler(mapSvc)

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			telemetry.MetricsInterceptor,
			l.UnaryServerInterceptor(),
		),
	)

	pb.RegisterMapServiceServer(s, handler)

	return &App{
		cfg:        cfg,
		log:        l,
		grpcServer: s,
	}
}

func (a *App) Run() error {
	shutdown, err := telemetry.Init("map-service", a.cfg.OtelCollectorURL)
	if err != nil {
		a.log.Error("failed to init telemetry", zap.Error(err))
	}
	defer shutdown()

	lis, err := net.Listen("tcp", ":"+a.cfg.GRPCPort)
	if err != nil {
		return err
	}

	go a.handleSignals()

	a.log.Info("Map gRPC Service started", zap.String("port", a.cfg.GRPCPort))
	return a.grpcServer.Serve(lis)
}

func (a *App) handleSignals() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	s := <-quit
	a.log.Info("Shutting down map-service", zap.String("signal", s.String()))
	a.grpcServer.GracefulStop()
}
