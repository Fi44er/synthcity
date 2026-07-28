package grpc

import (
	"context"

	"buf.build/go/protovalidate"
	pb "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	"github.com/Fi44er/synthcity/services/map-service/internal/service"
	"github.com/Fi44er/synthcity/services/map-service/internal/transport/grpc/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedMapServiceServer
	svc       *service.Router
	validator *protovalidate.Validator
	converter *converter.Converter
}

func NewHandler(svc *service.Router) *Handler {
	return &Handler{
		converter: &converter.Converter{},
		svc:       svc,
	}
}

func (h *Handler) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	start := h.converter.ToDomainCoord(req.Start)
	end := h.converter.ToDomainCoord(req.End)

	route, err := h.svc.GetRoute(ctx, start, end)
	if err != nil {
		return nil, err
	}

	routeRes := h.converter.ToProtoResponse(route)

	return routeRes, nil
}
