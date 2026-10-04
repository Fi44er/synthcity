package osm

import (
	"context"
	"sync"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

type StaticStats struct {
	TrafficSignals int
	Crossings      int
}

type StaticProcessor struct {
	repo domain.StaticRepository

	objChan   chan *domain.StaticObject
	batchSize int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	stats     StaticStats
}

func NewStaticProcessor() *StaticProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &StaticProcessor{
		objChan:   make(chan *domain.StaticObject, 1000), // буфер 1000 объектов
		batchSize: 100,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (p *StaticProcessor) Stats() StaticStats { return p.stats }

func (p *StaticProcessor) ProcessNode(n *osm.Node) *domain.StaticObject {
	obj := classifyNode(n)
	if obj == nil {
		return nil
	}

	switch obj.Type {
	case domain.TypeTrafficLight:
		p.stats.TrafficSignals++
	case domain.TypeCrossing:
		p.stats.Crossings++
	}

	return obj

	// TODO(T029): накапливать obj и сохранять в PostGIS пачками (p.repo.BulkSave).
}

func classifyNode(n *osm.Node) *domain.StaticObject {
	switch n.Tags.Find("highway") {
	case "traffic_signals":
		direction := n.Tags.Find("traffic_signals:direction")
		if direction == "" {
			direction = n.Tags.Find("direction") // если указано, куда светит
		}
		return &domain.StaticObject{
			ID:       int64(n.ID),
			Type:     domain.TypeTrafficLight,
			Subtype:  n.Tags.Find("traffic_signals"),
			Geometry: orb.Point{n.Lon, n.Lat},
			Properties: map[string]any{
				"direction": direction,
				"crossing":  n.Tags.Find("crossing"), // у светофора может быть и пешеходный переход
			},
		}

	case "crossing":
		return &domain.StaticObject{
			ID:       int64(n.ID),
			Type:     domain.TypeCrossing,
			Subtype:  crossingSubtype(n),
			Geometry: orb.Point{n.Lon, n.Lat},
			Properties: map[string]any{
				"markings": n.Tags.Find("crossing:markings"),
				"island":   n.Tags.Find("crossing:island"),
			},
		}
	}
	return nil
}

func crossingSubtype(n *osm.Node) string {
	v := n.Tags.Find("crossing")
	if v == "" {
		v = n.Tags.Find("crossing_ref")
	}
	if v == "" {
		return "unknown"
	}
	return v
}

func (p *StaticProcessor) ProcessWay(way *osm.Way, meta map[int64]*domain.Node) {

}

func (p *StaticProcessor) ProcessRelation(rel *osm.Relation, meta map[int64]*domain.Node) {}
