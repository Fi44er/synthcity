package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/Fi44er/synthcity/services/map-service/internal/infrastructure/osm"
	"github.com/Fi44er/synthcity/services/map-service/internal/service"
)

func main() {
	pbfPath := "../internal/data/test.pbf"

	log.Printf("=== SynthCity Map Service ===")
	log.Printf("Loading city map from: %s", pbfPath)

	start := time.Now()
	lgService := osm.NewLoader(pbfPath)
	ctx := context.Background()
	graph, err := lgService.LoadGraph(ctx)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to load graph: %v", err)
	}
	elapsed := time.Since(start)

	// --- Блок сбора статистики ---
	var (
		trafficLights = 0
		crossings     = 0
		totalEdges    = 0
		totalDistance = 0.0
		roadTypes     = make(map[string]int)
	)

	// Анализируем узлы
	for _, node := range graph.Nodes {
		switch node.Type {
		case domain.NodeTrafficLight:
			trafficLights++
		case domain.NodeCrossing:
			crossings++
		}
	}

	// Анализируем ребра
	for _, edges := range graph.Edges {
		for _, edge := range edges {
			totalEdges++
			totalDistance += edge.Distance
			roadTypes[edge.Highway]++
		}
	}

	// --- Вывод аналитики ---
	fmt.Println("\n============================================")
	fmt.Printf("✅ CITY GRAPH LOADED SUCCESSFULLY\n")
	fmt.Printf("⏱  Loading Time:    %v\n", elapsed)
	fmt.Println("============================================")

	fmt.Printf("📊 TOPOLOGY:\n")
	fmt.Printf("   • Road Nodes:     %d\n", len(graph.Nodes))
	fmt.Printf("   • Road Edges:     %d (directed segments)\n", totalEdges)
	fmt.Printf("   • Connectivity:   %.2f edges/node\n", float64(totalEdges)/float64(len(graph.Nodes)))
	fmt.Printf("   • Total Road Len: %.2f km\n", totalDistance/1000)

	fmt.Printf("\n🚦 INFRASTRUCTURE:\n")
	fmt.Printf("   • Traffic Lights: %d\n", trafficLights)
	fmt.Printf("   • Crossings:      %d\n", crossings)

	fmt.Printf("\n🛣  ROAD TYPES:\n")
	for roadType, count := range roadTypes {
		fmt.Printf("   • %-15s: %d segments\n", roadType, count)
	}
	fmt.Println("============================================")

	log.Println("Map Service is ready for routing requests.")

	router := service.NewRouter(graph)

	startPoint := domain.Coord{Lat: 51.77631542184869, Lon: 55.09960995617407} // Например, улица Кичигина 51.77631542184869, 55.09960995617407
	endPoint := domain.Coord{Lat: 51.787565576708786, Lon: 55.15414372347713}

	log.Printf("Finding path...")
	path, err := router.FindPath(context.Background(), startPoint, endPoint)
	if err != nil {
		log.Fatalf("Path error: %v", err)
	}

	fmt.Printf("\n🚀 ROUTE FOUND!\n")
	fmt.Printf("Nodes in path: %d\n", len(path))
	for i, p := range path {
		fmt.Printf("  [%d] %.6f, %.6f\n", i, p.Lat, p.Lon)
	}
}
