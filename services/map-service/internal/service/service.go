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

type Router struct {
	graph *domain.RoadGraph
}

func NewRouter(g *domain.RoadGraph) *Router {
	return &Router{graph: g}
}

func (r *Router) GetRoute(ctx context.Context, startCoord, endCoord domain.Coord) (*domain.Route, error) {
	start := time.Now()

	log := logger.FromContext(ctx).With(
		zap.String("operation", "FindPath"),
		zap.Float64("from_lat", startCoord.Lat),
		zap.Float64("from_lon", startCoord.Lon),
		zap.Float64("to_lat", endCoord.Lat),
		zap.Float64("to_lon", endCoord.Lon),
	)

	startID, err := r.graph.GetNearestNode(startCoord)
	if err != nil {
		log.Error("start node not found", zap.Error(err))
		return nil, err
	}
	goalID, err := r.graph.GetNearestNode(endCoord)
	if err != nil {
		log.Error("goal node not found", zap.Error(err))
		return nil, err
	}

	log.Debug("nodes identified", zap.Int64("start_id", startID), zap.Int64("goal_id", goalID))

	cameFrom := make(map[int64]int64)
	gScore := make(map[int64]float64)

	distToNode := make(map[int64]float64)
	// Аналогично для времени (с учетом штрафов)
	timeToNode := make(map[int64]float64)

	for id := range r.graph.Nodes {
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
			return r.reconstructRoute(cameFrom, distToNode, timeToNode, current), nil
		}

		for _, edge := range r.graph.Edges[current] {
			penalty := 0.0

			switch r.graph.Nodes[edge.ToID].Type {
			case domain.NodeTrafficLight:
				penalty = 15.0
			case domain.NodeCrossing:
				penalty = 3.0
			}
			weightWithPenalty := edge.Weight + penalty
			tentativeGScore := gScore[current] + weightWithPenalty

			if tentativeGScore < gScore[edge.ToID] {
				cameFrom[edge.ToID] = current
				gScore[edge.ToID] = tentativeGScore
				distToNode[edge.ToID] = edge.Distance
				timeToNode[edge.ToID] = weightWithPenalty

				distToGoal := domain.Haversine(r.graph.Nodes[edge.ToID].Point, r.graph.Nodes[goalID].Point)
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

func (r *Router) GetNearestNode(ctx context.Context, coord domain.Coord) (*domain.CoordRes, error) {
	log := logger.FromContext(ctx).With(
		zap.String("method", "GetNearestNode"),
		zap.Float64("lat", coord.Lat),
		zap.Float64("lon", coord.Lon),
	)
	nodeID, err := r.graph.GetNearestNode(coord)
	if err != nil {
		log.Error("start node not found", zap.Error(err))
		return nil, err
	}

	return &domain.CoordRes{
		Coord:  coord,
		NodeID: nodeID,
	}, nil
}

func (r *Router) reconstructRoute(cameFrom map[int64]int64, dists map[int64]float64, times map[int64]float64, current int64) *domain.Route {
	var path []domain.Coord
	var nodeIDs []int64
	var totalDist float64
	var totalTime float64

	for {
		path = append(path, r.graph.Nodes[current].Point)
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
