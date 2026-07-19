package app

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/Fi44er/synthcity/services/api-gateway/internal/config"
	"github.com/Fi44er/synthcity/services/api-gateway/internal/service"
	"github.com/Fi44er/synthcity/services/api-gateway/internal/transport/grpc"
)

type App struct {
	cfg     *config.Config
	log     *logger.Logger
	fiber   *fiber.App
	clients *grpc.Clients
}

func New(cfg *config.Config) *App {
	l := logger.New("api-gateway", "info")

	clients, err := grpc.NewClients(cfg)
	if err != nil {
		l.Fatal("Could not connect to gRPC services", zap.Error(err))
	}

	fiberApp := fiber.New(fiber.Config{
		DisableStartupMessage: false,
		AppName:               "SynthCity API Gateway",
	})

	return &App{
		cfg:     cfg,
		log:     l,
		fiber:   fiberApp,
		clients: clients,
	}
}

func (a *App) Run() error {
	shutdown, err := telemetry.Init("api-gateway", a.cfg.OtelCollectorURL)
	if err != nil {
		a.log.Error("failed to init telemetry", zap.Error(err))
	}
	defer shutdown()

	gwService := service.NewGatewayService(a.clients)

	a.setupMiddlewares()
	a.setupRoutes(gwService)

	// Graceful Shutdown
	go a.handleSignals()

	a.log.Info("Gateway started", zap.String("port", a.cfg.AppPort))
	return a.fiber.Listen(":" + a.cfg.AppPort)
}

func (a *App) setupMiddlewares() {
	a.fiber.Use(otelfiber.Middleware())

	a.fiber.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		telemetry.ActiveRequests.Add(c.Context(), 1, metric.WithAttributes(attribute.String("service_name", "api-gateway")))

		err := c.Next()

		telemetry.ActiveRequests.Add(c.Context(), -1, metric.WithAttributes(attribute.String("service_name", "api-gateway")))
		status := fmt.Sprintf("%d", c.Response().StatusCode())
		telemetry.RecordMetrics(c.Context(), start, "api-gateway", c.Path(), status)

		return err
	})

	a.fiber.Use(func(c *fiber.Ctx) error {
		l := logger.FromContext(c.UserContext())
		ctx := l.ContextWithLogger(c.UserContext())
		c.SetUserContext(ctx)
		return c.Next()
	})

	a.fiber.Use(requestid.New())
}

func (a *App) setupRoutes(gwService *service.GatewayService) {
	a.fiber.Get("/health", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	a.fiber.Use("/api", func(c *fiber.Ctx) error {
		newPath := strings.TrimPrefix(c.Path(), "/api")
		c.Path(newPath)
		return adaptor.HTTPHandler(gwService.HttpMux)(c)
	})
}

func (a *App) handleSignals() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	s := <-quit
	a.log.Info("Shutting down app", zap.String("signal", s.String()))

	if err := a.fiber.Shutdown(); err != nil {
		a.log.Error("Fiber shutdown error", zap.Error(err))
	}

	a.clients.Close()
	a.log.Sync()
}
