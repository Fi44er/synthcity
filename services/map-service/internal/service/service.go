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

func (r *Router) GetRoute(ctx context.Context, startCoord, endCoord domain.Coord) ([]domain.Coord, error) {
	start := time.Now()

	log := logger.FromContext(ctx).With(
		zap.String("operation", "FindPath"),
		zap.Float64("from_lat", startCoord.Lat),
		zap.Float64("from_lon", startCoord.Lon),
		zap.Float64("to_lat", endCoord.Lat),
		zap.Float64("to_lon", endCoord.Lon),
	)

	// 1. Находим ID ближайших узлов
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

	// 2. Алгоритм A*
	cameFrom := make(map[int64]int64)
	gScore := make(map[int64]float64) // Стоимость пути от старта до текущего узла

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
			path := r.reconstructPath(cameFrom, current)
			log.Info("path found",
				zap.Int("coords_count", len(path)),
				zap.Int("visited_nodes", visitedNodes),
				zap.Duration("duration", time.Since(start)),
			)

			telemetry.RecordMetrics(ctx, start, "map-service", "FindPath", "200")

			return path, nil
		}

		for _, edge := range r.graph.Edges[current] {
			penalty := 0.0

			// Если на целевом узле есть инфраструктура, добавляем "среднее время ожидания"
			switch r.graph.Nodes[edge.ToID].Type {
			case domain.NodeTrafficLight:
				penalty = 15.0 // В среднем мы стоим на светофоре 15 секунд
			case domain.NodeCrossing:
				penalty = 3.0 // Притормозить перед переходом
			}
			tentativeGScore := gScore[current] + edge.Weight + penalty // Вес — это время в секундах

			if tentativeGScore < gScore[edge.ToID] {
				cameFrom[edge.ToID] = current
				gScore[edge.ToID] = tentativeGScore

				// Эвристика: оставшееся время до цели по прямой
				distToGoal := domain.Haversine(r.graph.Nodes[edge.ToID].Point, r.graph.Nodes[goalID].Point)
				hScore := distToGoal / 30.0 // Предполагаем 30 м/с (108 км/ч) как оптимистичный лимит

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

func (r *Router) reconstructPath(cameFrom map[int64]int64, current int64) []domain.Coord {
	var path []domain.Coord
	for {
		path = append([]domain.Coord{r.graph.Nodes[current].Point}, path...)
		prev, ok := cameFrom[current]
		if !ok {
			break
		}
		current = prev
	}
	return path
}
