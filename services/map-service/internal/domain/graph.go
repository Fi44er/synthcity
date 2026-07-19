package domain

import (
	"fmt"
	"math"
)

type Coord struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

func Haversine(c1, c2 Coord) float64 {
	const R = 6371000 // Радиус Земли
	phi1 := c1.Lat * math.Pi / 180
	phi2 := c2.Lat * math.Pi / 180
	dPhi := (c2.Lat - c1.Lat) * math.Pi / 180
	dLambda := (c2.Lon - c1.Lon) * math.Pi / 180

	a := math.Sin(dPhi/2)*math.Sin(dPhi/2) +
		math.Cos(phi1)*math.Cos(phi2)*
			math.Sin(dLambda/2)*math.Sin(dLambda/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

type NodeType int8

const (
	NodeRegular      NodeType = iota
	NodeTrafficLight          // Светофор
	NodeCrossing              // Пешеходный переход
	NodeJunction              // Крупная развязка
)

type Node struct {
	ID    int64
	Point Coord
	Type  NodeType
}

type Edge struct {
	ToID     int64   // ID узла назначения
	Distance float64 // Длина в метрах
	MaxSpeed float64 // Макс. скорость в метрах в секунду (m/s)
	Weight   float64 // Вес для A* (время проезда в секундах)
	Lanes    int     // Количество полос
	Highway  string  // Тип дороги (motorway, primary...)
}

type RoadGraph struct {
	Nodes map[int64]*Node
	Edges map[int64][]*Edge
}

func (g *RoadGraph) GetNearestNode(point Coord) (int64, error) {
	var nearestID int64
	minDist := math.MaxFloat64
	found := false

	for id, node := range g.Nodes {
		dist := Haversine(point, node.Point)
		if dist < minDist {
			minDist = dist
			nearestID = id
			found = true
		}
	}

	if !found {
		return 0, fmt.Errorf("no nodes in graph")
	}

	if minDist > 1000 {
		return 0, fmt.Errorf("point is too far from road network (%.0f meters away)", minDist)
	}

	return nearestID, nil
}
