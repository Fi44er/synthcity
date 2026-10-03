package domain

import (
	"fmt"

	"github.com/dhconnelly/rtreego"
)

type spatialNode struct {
	id    int64
	point rtreego.Point
}

func (s *spatialNode) Bounds() rtreego.Rect { return s.point.ToRect(0.0001) }

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
