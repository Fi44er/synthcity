package osm

import (
	"strconv"
	"strings"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/osm"
)

const (
	DefaultSpeedCity    = 60.0 // км/ч
	DefaultSpeedHighway = 110.0
	DefaultLanes        = 1
)

type GraphProcessor struct {
	graph *domain.RoadGraph
}

func NewGraphProcessor(g *domain.RoadGraph) *GraphProcessor {
	return &GraphProcessor{graph: g}
}

func (p *GraphProcessor) ProcessWay(way *osm.Way, meta map[int64]*domain.Node) {
	highway := way.Tags.Find("highway")
	if highway == "" || !isDriveable(highway) {
		return
	}

	speedMS := parseMaxSpeed(way.Tags.Find("maxspeed"), highway)
	lanes := parseLanes(way.Tags.Find("lanes"), highway)
	isOneWay := way.Tags.Find("oneway") == "yes" || way.Tags.Find("oneway") == "1" || way.Tags.Find("oneway") == "reverse"

	for i := 0; i < len(way.Nodes.NodeIDs())-1; i++ {
		fromID := way.Nodes.NodeIDs()[i]
		toID := way.Nodes.NodeIDs()[i+1]

		n1, ok1 := meta[int64(fromID)]
		n2, ok2 := meta[int64(toID)]

		if ok1 && ok2 {
			dist := domain.Haversine(n1.Point, n2.Point)

			p.graph.AddNode(n1)
			p.graph.AddNode(n2)

			edge := &domain.Edge{
				ToID:     int64(toID),
				Distance: dist,
				MaxSpeed: speedMS,
				Weight:   dist / speedMS,
				Lanes:    lanes,
				Highway:  highway,
			}
			p.graph.AddEdge(int64(fromID), edge)

			if !isOneWay {
				backEdge := *edge
				backEdge.ToID = int64(fromID)
				p.graph.AddEdge(int64(toID), &backEdge)
			}
		}
	}
}

func isDriveable(h string) bool {
	valid := map[string]bool{
		"motorway": true, "trunk": true, "primary": true,
		"secondary": true, "tertiary": true, "residential": true,
		"living_street": true, "motorway_link": true, "service": true,
	}
	return valid[h]
}

func parseMaxSpeed(tag string, highway string) float64 {
	var kmh float64
	if tag == "" {
		switch highway {
		case "motorway":
			kmh = DefaultSpeedHighway
		case "residential":
			kmh = 40.0
		case "living_street":
			kmh = 20.0
		default:
			kmh = DefaultSpeedCity
		}
	} else {
		clean := strings.Split(tag, " ")[0]
		parsed, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			kmh = DefaultSpeedCity
		} else {
			kmh = parsed
		}
	}
	return kmh / 3.6
}

func parseLanes(tag string, highway string) int {
	if tag == "" {
		switch highway {
		case "motorway":
			return 3
		case "primary", "secondary":
			return 2
		default:
			return DefaultLanes
		}
	}
	l, _ := strconv.Atoi(tag)
	if l <= 0 {
		return 1
	}
	return l
}
