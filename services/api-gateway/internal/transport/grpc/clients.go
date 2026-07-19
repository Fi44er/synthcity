package grpc

import (
	"time"

	pbMap "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	pbHello "github.com/Fi44er/synthcity/api/gen/go/test/v1"
	"github.com/Fi44er/synthcity/services/api-gateway/internal/config"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Clients struct {
	HelloClient pbHello.HelloServiceClient
	MapClient   pbMap.MapServiceClient
	conns       []*grpc.ClientConn
}

const retryPolicy = `{
	"methodConfig": [{
		"name": [{"service": ""}],
		"retryPolicy": {
			"maxAttempts": 3,
			"initialBackoff": "0.1s",
			"maxBackoff": "1s",
			"backoffMultiplier": 1.6,
			"retryableStatusCodes": ["UNAVAILABLE", "INTERNAL"]
		}
	}]
}`

func NewClients(cfg *config.Config) (*Clients, error) {
	kacp := keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             time.Second,
		PermitWithoutStream: false,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithDefaultServiceConfig(retryPolicy),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}

	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, opts...)
	}

	hConn, err := dial(cfg.HelloServiceAddr)
	if err != nil {
		return nil, err
	}

	mConn, err := dial(cfg.MapServiceAddr)
	if err != nil {
		return nil, err
	}

	return &Clients{
		HelloClient: pbHello.NewHelloServiceClient(hConn),
		MapClient:   pbMap.NewMapServiceClient(mConn),
		conns:       []*grpc.ClientConn{hConn, mConn},
	}, nil
}

func (c *Clients) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}
