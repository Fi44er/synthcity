package grpc

import (
	"context"

	pb "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/Fi44er/synthcity/services/map-service/internal/service"
)

type Handler struct {
	pb.UnimplementedMapServiceServer
	svc *service.Router
}

func NewHandler(svc *service.Router) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	start := domain.Coord{Lat: req.Start.Lat, Lon: req.Start.Lon}
	end := domain.Coord{Lat: req.End.Lat, Lon: req.End.Lon}

	path, err := h.svc.GetRoute(ctx, start, end)
	if err != nil {
		return nil, err
	}

	points := make([]*pb.LatLng, len(path))
	for i, p := range path {
		points[i] = &pb.LatLng{Lat: p.Lat, Lon: p.Lon}
	}

	return &pb.GetRouteResponse{Points: points}, nil
}
