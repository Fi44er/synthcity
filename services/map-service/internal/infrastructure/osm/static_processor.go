package osm

import (
	"context"
	"fmt"
	"sync"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

type StaticProcessor struct {
	repo domain.StaticRepository

	objChan   chan *domain.StaticObject
	batchSize int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
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

func (p *StaticProcessor) Test() {

}

func (p *StaticProcessor) ProcessNode(n *osm.Node) {
	switch n.Tags.Find("highway") {
	case "traffic_signals":
		obj := &domain.StaticObject{
			ID:       int64(n.ID),
			Type:     domain.TypeTrafficLight,
			Geometry: orb.Point{n.Lon, n.Lat},
			Properties: map[string]any{
				"direction": n.Tags.Find("direction"), // если указано, куда светит
			},
		}
		fmt.Printf("%v\n", obj)
	}
	// p.repo.BulkSave(context.Background(), []*domain.StaticObject{obj})
}

func (p *StaticProcessor) ProcessWay(way *osm.Way, meta map[int64]*domain.Node) {

}

func (p *StaticProcessor) ProcessRelation(rel *osm.Relation, meta map[int64]*domain.Node) {}
