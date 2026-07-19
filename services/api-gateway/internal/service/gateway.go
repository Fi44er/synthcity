package service

import (
	"context"
	"log"
	"net/http"

	pbMap "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	pbHello "github.com/Fi44er/synthcity/api/gen/go/test/v1"

	"github.com/Fi44er/synthcity/services/api-gateway/internal/transport/grpc"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

type GatewayService struct {
	HttpMux *runtime.ServeMux
}

func NewGatewayService(clients *grpc.Clients) *GatewayService {
	gwmux := runtime.NewServeMux(
		runtime.WithErrorHandler(func(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
			runtime.DefaultHTTPErrorHandler(ctx, mux, marshaler, w, r, err)
		}),
	)

	ctx := context.Background()

	if err := pbHello.RegisterHelloServiceHandlerClient(ctx, gwmux, clients.HelloClient); err != nil {
		log.Fatalf("Failed to register HelloService: %v", err)
	}

	if err := pbMap.RegisterMapServiceHandlerClient(ctx, gwmux, clients.MapClient); err != nil {
		log.Fatalf("Failed to register MapService: %v", err)
	}

	return &GatewayService{
		HttpMux: gwmux,
	}
}
