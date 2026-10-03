package grpc

import (
	"context"

	"buf.build/go/protovalidate"
	pb "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	service "github.com/Fi44er/synthcity/services/map-service/internal/service/route"
	"github.com/Fi44er/synthcity/services/map-service/internal/transport/grpc/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedMapServiceServer
	svc       *service.MapService
	validator *protovalidate.Validator
	converter *converter.Converter
}

func NewHandler(svc *service.MapService) *Handler {
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

	routeRes := h.converter.ToRouteProtoResponse(route)

	return routeRes, nil
}

func (h *Handler) UpdateEdgeWeight(ctx context.Context, req *pb.UpdateEdgeWeightRequest) (*pb.UpdateEdgeWeightResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := h.svc.UpdateEdgeWeight(ctx, req.FromNodeId, req.ToNodeId, req.WeightMultiplier); err != nil {
		return nil, err
	}

	return &pb.UpdateEdgeWeightResponse{}, nil
}

func (h *Handler) GetJunctionInfo(ctx context.Context, req *pb.GetJunctionInfoRequest) (*pb.GetJunctionInfoResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	junction, err := h.svc.GetJunction(ctx, req.NodeId)
	if err != nil {
		return nil, err
	}

	junctionRes := h.converter.ToJunctionProtoResponse(&junction)

	return junctionRes, nil
}

func (h *Handler) GetNearestNode(ctx context.Context, req *pb.GetNearestNodeRequest) (*pb.GetNearestNodeResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	coord := h.converter.ToDomainCoord(req.Location)
	nearestNode, err := h.svc.GetNearestNode(ctx, coord)
	if err != nil {
		return nil, err
	}

	nearestNodeRes := h.converter.ToNearestNodeProtoResponse(nearestNode)

	return nearestNodeRes, nil
}
