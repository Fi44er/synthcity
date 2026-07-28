package domain

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/dhconnelly/rtreego"
	"github.com/vmihailenco/msgpack/v5"
)

// --- Базовые типы ---

type Coord struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type CoordRes struct {
	Coord  Coord `json:"coord"`
	NodeID int64 `json:"node_id"`
}

type NodeType int8

const (
	NodeRegular NodeType = iota
	NodeTrafficLight
	NodeCrossing
)

type Node struct {
	ID    int64
	Point Coord
	Type  NodeType
}

type Edge struct {
	ToID     int64
	Distance float64
	MaxSpeed float64
	Weight   float64
	Lanes    int
	Highway  string
}

type Route struct {
	Points   []Coord
	NodeIDs  []int64
	Distance float64
	Duration float64
}

type RoadGraph struct {
	Nodes map[int64]*Node   `msgpack:"nodes"`
	Edges map[int64][]*Edge `msgpack:"edges"`

	tree *rtreego.Rtree `msgpack:"-"`
	// Мьютекс нужен только если мы планируем обновлять граф на лету (аварии)
	// Если граф только читается — можно убрать для скорости
	mu sync.RWMutex `msgpack:"-"`
}

func NewRoadGraph() *RoadGraph {
	return &RoadGraph{
		Nodes: make(map[int64]*Node),
		Edges: make(map[int64][]*Edge),
		tree:  rtreego.NewTree(2, 25, 50),
	}
}

type spatialNode struct {
	id    int64
	point rtreego.Point
}

func (s *spatialNode) Bounds() rtreego.Rect { return s.point.ToRect(0.0001) }

func (g *RoadGraph) AddNode(n *Node) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.Nodes[n.ID] = n
	g.tree.Insert(&spatialNode{
		id:    n.ID,
		point: rtreego.Point{n.Point.Lon, n.Point.Lat},
	})
}

func (g *RoadGraph) AddEdge(fromID int64, edge *Edge) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Edges[fromID] = append(g.Edges[fromID], edge)
}

func (g *RoadGraph) GetNearestNode(p Coord) (int64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	q := rtreego.Point{p.Lon, p.Lat}
	nearest := g.tree.NearestNeighbor(q)
	if nearest == nil {
		return 0, fmt.Errorf("no nodes found")
	}

	sn := nearest.(*spatialNode)

	dist := Haversine(p, g.Nodes[sn.id].Point)
	if dist > 1000 {
		return 0, fmt.Errorf("nearest node too far: %.0fm", dist)
	}

	return sn.id, nil
}

func (g *RoadGraph) SaveBinary(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	enc := msgpack.NewEncoder(writer)

	err = enc.Encode(g)
	if err != nil {
		return err
	}
	return writer.Flush()
}

func LoadBinary(path string) (*RoadGraph, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	g := NewRoadGraph()

	reader := bufio.NewReader(file)

	dec := msgpack.NewDecoder(reader)
	if err := dec.Decode(g); err != nil {
		return nil, err
	}

	spatialItems := make([]rtreego.Spatial, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		spatialItems = append(spatialItems, &spatialNode{
			id:    n.ID,
			point: rtreego.Point{n.Point.Lon, n.Point.Lat},
		})
	}

	g.tree = rtreego.NewTree(2, 25, 50, spatialItems...)

	return g, nil
}

func Haversine(c1, c2 Coord) float64 {
	const R = 6371000
	phi1 := c1.Lat * math.Pi / 180
	phi2 := c2.Lat * math.Pi / 180
	dPhi := (c2.Lat - c1.Lat) * math.Pi / 180
	dLambda := (c2.Lon - c1.Lon) * math.Pi / 180

	a := math.Sin(dPhi/2)*math.Sin(dPhi/2) +
		math.Cos(phi1)*math.Cos(phi2)*
			math.Sin(dLambda/2)*math.Sin(dLambda/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
