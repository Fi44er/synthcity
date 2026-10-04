package service

import (
	"context"
	"math"
	"testing"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
)

// Узлы на сетке ~100–150 м; веса рёбер заданы вручную (секунды).
var (
	pStart = domain.Coord{Lat: 51.000, Lon: 55.000}
	pMid   = domain.Coord{Lat: 51.001, Lon: 55.001}
	pAlt   = domain.Coord{Lat: 50.999, Lon: 55.001}
	pGoal  = domain.Coord{Lat: 51.000, Lon: 55.002}
)

func edge(to int64) *domain.Edge {
	return &domain.Edge{ToID: to, Distance: 100, MaxSpeed: 10, Weight: 10, Lanes: 1, Highway: "residential"}
}

// lineGraph: 1 → 2 → 4, единственный путь; тип узла 2 задаётся параметром.
func lineGraph(midType domain.NodeType) *domain.RoadGraph {
	g := domain.NewRoadGraph()
	g.AddNode(&domain.Node{ID: 1, Point: pStart, Type: domain.NodeRegular})
	g.AddNode(&domain.Node{ID: 2, Point: pMid, Type: midType})
	g.AddNode(&domain.Node{ID: 4, Point: pGoal, Type: domain.NodeRegular})
	g.AddEdge(1, edge(2))
	g.AddEdge(2, edge(4))
	return g
}

func TestGetRoute_PenaltyForTrafficLightAndCrossing(t *testing.T) {
	ctx := context.Background()

	duration := func(typ domain.NodeType) float64 {
		r, err := NewMapService(lineGraph(typ)).GetRoute(ctx, pStart, pGoal)
		if err != nil {
			t.Fatalf("GetRoute: %v", err)
		}
		return r.Duration
	}

	base := duration(domain.NodeRegular)
	if math.Abs(base-20) > 1e-9 {
		t.Fatalf("базовая длительность = %v, want 20", base)
	}
	if got := duration(domain.NodeTrafficLight); math.Abs(got-(base+TrafficLightPenaltySec)) > 1e-9 {
		t.Errorf("со светофором: %v, want %v", got, base+TrafficLightPenaltySec)
	}
	if got := duration(domain.NodeCrossing); math.Abs(got-(base+CrossingPenaltySec)) > 1e-9 {
		t.Errorf("с переходом: %v, want %v", got, base+CrossingPenaltySec)
	}
}

// Два одинаковых по длине пути: через светофор (узел 2) и через обычный узел (узел 3).
func TestGetRoute_PrefersPathWithoutTrafficLight(t *testing.T) {
	g := domain.NewRoadGraph()
	g.AddNode(&domain.Node{ID: 1, Point: pStart, Type: domain.NodeRegular})
	g.AddNode(&domain.Node{ID: 2, Point: pMid, Type: domain.NodeTrafficLight})
	g.AddNode(&domain.Node{ID: 3, Point: pAlt, Type: domain.NodeRegular})
	g.AddNode(&domain.Node{ID: 4, Point: pGoal, Type: domain.NodeRegular})
	g.AddEdge(1, edge(2))
	g.AddEdge(2, edge(4))
	g.AddEdge(1, edge(3))
	g.AddEdge(3, edge(4))

	r, err := NewMapService(g).GetRoute(context.Background(), pStart, pGoal)
	if err != nil {
		t.Fatalf("GetRoute: %v", err)
	}

	want := []int64{1, 3, 4}
	if len(r.NodeIDs) != len(want) {
		t.Fatalf("NodeIDs = %v, want %v", r.NodeIDs, want)
	}
	for i := range want {
		if r.NodeIDs[i] != want[i] {
			t.Fatalf("NodeIDs = %v, want %v", r.NodeIDs, want)
		}
	}
}
