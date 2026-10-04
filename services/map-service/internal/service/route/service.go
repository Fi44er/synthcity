package service

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/Fi44er/synthcity/services/map-service/pkg/utils"
	"go.uber.org/zap"
)

const (
	TrafficLightPenaltySec = 15.0
	CrossingPenaltySec     = 3.0
)

type MapService struct {
	graph *domain.RoadGraph
}

func NewMapService(g *domain.RoadGraph) *MapService {
	return &MapService{graph: g}
}

func (s *MapService) GetRoute(ctx context.Context, startCoord, endCoord domain.Coord) (*domain.Route, error) {
	start := time.Now()

	log := logger.FromContext(ctx).With(
		zap.String("operation", "FindPath"),
		zap.Float64("from_lat", startCoord.Lat),
		zap.Float64("from_lon", startCoord.Lon),
		zap.Float64("to_lat", endCoord.Lat),
		zap.Float64("to_lon", endCoord.Lon),
	)

	startID, err := s.graph.GetNearestNode(startCoord)
	if err != nil {
		log.Error("start node not found", zap.Error(err))
		return nil, err
	}
	goalID, err := s.graph.GetNearestNode(endCoord)
	if err != nil {
		log.Error("goal node not found", zap.Error(err))
		return nil, err
	}

	log.Debug("nodes identified", zap.Int64("start_id", startID), zap.Int64("goal_id", goalID))

	cameFrom := make(map[int64]int64)
	gScore := make(map[int64]float64)

	distToNode := make(map[int64]float64)
	timeToNode := make(map[int64]float64)

	for id := range s.graph.Nodes {
		gScore[id] = math.MaxFloat64
	}
	gScore[startID] = 0

	pq := utils.NewPriorityQueue()
	heap.Init(pq)
	heap.Push(pq, &utils.Item{NodeID: startID, Priority: 0})

	visitedNodes := 0

	for pq.Len() > 0 {
		visitedNodes++
		current := heap.Pop(pq).(*utils.Item).NodeID

		if current == goalID {
			log.Info("path found",
				zap.Int("visited_nodes", visitedNodes),
				zap.Duration("duration", time.Since(start)),
			)

			telemetry.RecordMetrics(ctx, start, "map-service", "FindPath", "200")
			return s.reconstructRoute(cameFrom, distToNode, timeToNode, current), nil
		}

		for _, edge := range s.graph.Edges[current] {
			penalty := 0.0

			switch s.graph.Nodes[edge.ToID].Type {
			case domain.NodeTrafficLight:
				penalty = TrafficLightPenaltySec
			case domain.NodeCrossing:
				penalty = CrossingPenaltySec
			}
			weightWithPenalty := edge.Weight + penalty
			tentativeGScore := gScore[current] + weightWithPenalty

			if tentativeGScore < gScore[edge.ToID] {
				cameFrom[edge.ToID] = current
				gScore[edge.ToID] = tentativeGScore
				distToNode[edge.ToID] = edge.Distance
				timeToNode[edge.ToID] = weightWithPenalty

				distToGoal := domain.Haversine(s.graph.Nodes[edge.ToID].Point, s.graph.Nodes[goalID].Point)
				hScore := distToGoal / 30.0 // эвристика: оставшееся время до цели по прямой

				heap.Push(pq, &utils.Item{
					NodeID:   edge.ToID,
					Priority: tentativeGScore + hScore,
				})
			}
		}
	}

	log.Warn("path not found", zap.Int("visited_nodes", visitedNodes))
	telemetry.RecordMetrics(ctx, start, "map-service", "FindPath", "404")
	return nil, fmt.Errorf("path not found")
}

func (s *MapService) GetNearestNode(ctx context.Context, coord domain.Coord) (*domain.CoordRes, error) {
	log := logger.FromContext(ctx).With(
		zap.String("method", "GetNearestNode"),
		zap.Float64("lat", coord.Lat),
		zap.Float64("lon", coord.Lon),
	)
	nodeID, err := s.graph.GetNearestNode(coord)
	if err != nil {
		log.Error("start node not found", zap.Error(err))
		return nil, err
	}

	return &domain.CoordRes{
		Coord:  coord,
		NodeID: nodeID,
	}, nil
}

func (s *MapService) GetJunction(ctx context.Context, id int64) (domain.JunctionInfo, error) {
	nodeID, neighbors, nodeType, err := s.graph.GetJunctionInfo(id)
	if err != nil {
		return domain.JunctionInfo{}, err
	}

	return domain.JunctionInfo{
		ID:               *nodeID,
		Type:             nodeType,
		ConnectedNodeIDs: neighbors,
	}, nil
}

func (s *MapService) UpdateEdgeWeight(ctx context.Context, from, to int64, mult float64) error {
	log := logger.FromContext(ctx)
	log.Info("updating edge weight",
		zap.Int64("from", from),
		zap.Int64("to", to),
		zap.Float64("mult", mult))

	return s.graph.UpdateEdgeWeight(from, to, mult)
}

func (s *MapService) reconstructRoute(cameFrom map[int64]int64, dists map[int64]float64, times map[int64]float64, current int64) *domain.Route {
	var path []domain.Coord
	var nodeIDs []int64
	var totalDist float64
	var totalTime float64

	for {
		path = append(path, s.graph.Nodes[current].Point)
		nodeIDs = append(nodeIDs, current)

		totalDist += dists[current]
		totalTime += times[current]

		prev, ok := cameFrom[current]
		if !ok {
			break
		}
		current = prev
	}

	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
		nodeIDs[i], nodeIDs[j] = nodeIDs[j], nodeIDs[i]
	}

	return &domain.Route{
		Points:   path,
		NodeIDs:  nodeIDs,
		Distance: totalDist,
		Duration: totalTime,
	}
}
