package osm

import (
	"testing"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/orb"
	"github.com/paulmach/osm"
)

func tags(kv ...string) osm.Tags {
	t := make(osm.Tags, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		t = append(t, osm.Tag{Key: kv[i], Value: kv[i+1]})
	}
	return t
}

func TestClassifyNode(t *testing.T) {
	tests := []struct {
		name        string
		tags        osm.Tags
		wantNil     bool
		wantType    domain.StaticObjectType
		wantSubtype string
	}{
		{"светофор", tags("highway", "traffic_signals"), false, domain.TypeTrafficLight, ""},
		{"переход со светофором", tags("highway", "crossing", "crossing", "traffic_signals"), false, domain.TypeCrossing, "traffic_signals"},
		{"нерегулируемый переход", tags("highway", "crossing", "crossing", "uncontrolled"), false, domain.TypeCrossing, "uncontrolled"},
		{"размеченный переход", tags("highway", "crossing", "crossing", "marked"), false, domain.TypeCrossing, "marked"},
		{"зебра", tags("highway", "crossing", "crossing", "zebra"), false, domain.TypeCrossing, "zebra"},
		{"переход без подтипа", tags("highway", "crossing"), false, domain.TypeCrossing, "unknown"},
		{"старая схема crossing_ref", tags("highway", "crossing", "crossing_ref", "zebra"), false, domain.TypeCrossing, "zebra"},
		{"светофор с тегом crossing остаётся светофором", tags("highway", "traffic_signals", "crossing", "traffic_signals"), false, domain.TypeTrafficLight, ""},
		{"ЖД-переезд — не наш объект", tags("railway", "level_crossing"), true, "", ""},
		{"опечатка в ключе не срабатывает", tags("higway", "traffic_signals"), true, "", ""},
		{"узел без тегов", nil, true, "", ""},
		{"прочий highway-узел", tags("highway", "bus_stop"), true, "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := &osm.Node{ID: 42, Lat: 51.77, Lon: 55.10, Tags: tc.tags}
			got := classifyNode(n)

			if tc.wantNil {
				if got != nil {
					t.Fatalf("ожидали nil, получили %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("ожидали объект, получили nil")
			}
			if got.Type != tc.wantType {
				t.Errorf("Type = %q, want %q", got.Type, tc.wantType)
			}
			if got.Subtype != tc.wantSubtype {
				t.Errorf("Subtype = %q, want %q", got.Subtype, tc.wantSubtype)
			}
			if got.ID != 42 {
				t.Errorf("ID = %d, want 42", got.ID)
			}
			if p, ok := got.Geometry.(orb.Point); !ok || p[0] != 55.10 || p[1] != 51.77 {
				t.Errorf("Geometry = %v, want Point{lon=55.10, lat=51.77}", got.Geometry)
			}
		})
	}
}

func TestProcessNode_Counters(t *testing.T) {
	p := NewStaticProcessor()

	for _, n := range []*osm.Node{
		{ID: 1, Tags: tags("highway", "traffic_signals")},
		{ID: 2, Tags: tags("highway", "traffic_signals")},
		{ID: 3, Tags: tags("highway", "crossing", "crossing", "zebra")},
		{ID: 4, Tags: tags("amenity", "cafe")},
	} {
		p.ProcessNode(n)
	}

	got := p.Stats()
	if got.TrafficSignals != 2 || got.Crossings != 1 {
		t.Fatalf("Stats = %+v, want {TrafficSignals:2 Crossings:1}", got)
	}
}
