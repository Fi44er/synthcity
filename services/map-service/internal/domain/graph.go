package domain

import (
	"fmt"
	"sync"

	"github.com/dhconnelly/rtreego"
)

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

func (g *RoadGraph) AddNode(n *Node) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Nodes[n.ID]; exists {
		return
	}
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

func (g *RoadGraph) GetJunctionInfo(id int64) (*int64, []int64, NodeType, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, exists := g.Nodes[id]
	if !exists {
		return nil, nil, 0, fmt.Errorf("node %d not found", id)
	}

	var neighbors []int64
	for _, edge := range g.Edges[id] {
		neighbors = append(neighbors, edge.ToID)
	}

	return &node.ID, neighbors, node.Type, nil
}

func (g *RoadGraph) UpdateEdgeWeight(fromID, toID int64, multiplier float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	edges, exists := g.Edges[fromID]
	if !exists {
		return fmt.Errorf("from_node %d not found", fromID)
	}

	found := false
	for _, edge := range edges {
		if edge.ToID == toID {
			edge.Weight = (edge.Distance / edge.MaxSpeed) * multiplier
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("edge %d -> %d not found", fromID, toID)
	}

	return nil
}
