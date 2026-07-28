package converter

import (
	pb "github.com/Fi44er/synthcity/api/gen/go/map/v1"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
)

type Converter struct{}

func (c *Converter) ToDomainCoord(pb *pb.LatLng) domain.Coord {
	return domain.Coord{Lat: pb.Lat, Lon: pb.Lon}
}

func (c *Converter) ToProtoCoord(coord domain.Coord) *pb.LatLng {
	return &pb.LatLng{Lat: coord.Lat, Lon: coord.Lon}
}

func (c *Converter) ToProtoResponse(route *domain.Route) *pb.GetRouteResponse {
	points := make([]*pb.LatLng, len(route.Points))
	for i, p := range route.Points {
		points[i] = c.ToProtoCoord(p)
	}

	return &pb.GetRouteResponse{
		Points:          points,
		NodeIds:         route.NodeIDs,
		DistanceMeters:  route.Distance,
		DurationSeconds: route.Duration,
	}
}
