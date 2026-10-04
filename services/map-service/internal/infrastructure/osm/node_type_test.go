package osm

import (
	"testing"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/osm"
)

func TestProcessNode_ReturnsObjectAndCounts(t *testing.T) {
	p := NewStaticProcessor()

	if obj := p.ProcessNode(&osm.Node{ID: 1, Tags: tags("highway", "traffic_signals")}); obj == nil || obj.Type != domain.TypeTrafficLight {
		t.Fatalf("светофор: obj = %+v", obj)
	}
	p.ProcessNode(&osm.Node{ID: 2, Tags: tags("highway", "crossing", "crossing", "zebra")})
	if obj := p.ProcessNode(&osm.Node{ID: 3, Tags: tags("amenity", "cafe")}); obj != nil {
		t.Fatalf("кафе должно дать nil, получили %+v", obj)
	}

	if got := p.Stats(); got.TrafficSignals != 1 || got.Crossings != 1 {
		t.Fatalf("Stats = %+v", got)
	}
}

// Сквозная проверка «теги → тип узла графа»: теги разбираются одним classifyNode.
func TestGraphNodeType_FromTags(t *testing.T) {
	tests := []struct {
		name string
		tags osm.Tags
		want domain.NodeType
	}{
		{"светофор", tags("highway", "traffic_signals"), domain.NodeTrafficLight},
		{"переход", tags("highway", "crossing", "crossing", "zebra"), domain.NodeCrossing},
		{"светофор с crossing — светофор", tags("highway", "traffic_signals", "crossing", "traffic_signals"), domain.NodeTrafficLight},
		{"узел без тегов", nil, domain.NodeRegular},
		{"прочий highway-узел", tags("highway", "bus_stop"), domain.NodeRegular},
		{"ЖД-переезд", tags("railway", "level_crossing"), domain.NodeRegular},
		{"ключ с другим регистром", tags("Highway", "traffic_signals"), domain.NodeRegular},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := graphNodeType(classifyNode(&osm.Node{ID: 1, Tags: tc.tags}))
			if got != tc.want {
				t.Errorf("graphNodeType = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGraphNodeType_NonRoutingObjectsAreRegular(t *testing.T) {
	if got := graphNodeType(&domain.StaticObject{Type: domain.TypeBuilding}); got != domain.NodeRegular {
		t.Errorf("здание: %d, want NodeRegular", got)
	}
	if got := graphNodeType(nil); got != domain.NodeRegular {
		t.Errorf("nil: %d, want NodeRegular", got)
	}
}
