package osm

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/qedus/osmpbf"
	"go.uber.org/zap"
)

type ILoader interface {
	LoadGraph(ctx context.Context) (*domain.RoadGraph, error)
}

type Loader struct {
	osmPath string
}

func NewLoader(osmPath string) ILoader {
	return &Loader{osmPath: osmPath}
}

// TODO вынести в бд
const (
	DefaultSpeedCity    = 60.0 // км/ч
	DefaultSpeedHighway = 110.0
	DefaultLanes        = 1
)

func (l *Loader) LoadGraph(ctx context.Context) (*domain.RoadGraph, error) {
	pbfPath := l.osmPath

	start := time.Now()

	log := logger.FromContext(ctx).With(
		zap.String("operation", "LoadGraph"),
		zap.String("pbf_path", pbfPath),
	)

	log.Info("starting graph loading")

	f, err := os.Open(pbfPath)
	if err != nil {
		log.Error("failed to open pbf file", zap.Error(err))
		return nil, fmt.Errorf("open pbf: %w", err)
	}
	defer f.Close()

	d := osmpbf.NewDecoder(f)
	if err := d.Start(runtime.GOMAXPROCS(0)); err != nil {
		log.Error("failed to start pbf decoder", zap.Error(err))
		return nil, fmt.Errorf("start decoder: %w", err)
	}

	graph := &domain.RoadGraph{
		Nodes: make(map[int64]*domain.Node),
		Edges: make(map[int64][]*domain.Edge),
	}
	allNodeMeta := make(map[int64]*domain.Node)
	var rawNodes, rawWays int64

	for {
		v, err := d.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error("error during pbf decoding", zap.Error(err))
			return nil, err
		}

		switch n := v.(type) {
		case *osmpbf.Node:
			rawNodes++
			nodeType := domain.NodeRegular
			switch n.Tags["highway"] {
			case "traffic_signals":
				nodeType = domain.NodeTrafficLight
			case "crossing":
				nodeType = domain.NodeCrossing
			}

			allNodeMeta[n.ID] = &domain.Node{
				ID:    n.ID,
				Point: domain.Coord{Lat: n.Lat, Lon: n.Lon},
				Type:  nodeType,
			}

		case *osmpbf.Way:
			rawWays++
			highwayType, isRoad := n.Tags["highway"]
			if isRoad && isDriveable(highwayType) {
				processWay(graph, n, allNodeMeta, highwayType)
			}
		}

		if (rawNodes+rawWays)%10000 == 0 {
			log.Debug("processing progress",
				zap.Int64("nodes_read", rawNodes),
				zap.Int64("ways_read", rawWays),
			)
		}
	}

	telemetry.RecordMetrics(ctx, start, "map-service", "LoadGraph", "success")

	return graph, nil
}

func processWay(graph *domain.RoadGraph, way *osmpbf.Way, meta map[int64]*domain.Node, highway string) {
	speedMS := parseMaxSpeed(way.Tags["maxspeed"], highway)
	lanes := parseLanes(way.Tags["lanes"], highway)
	isOneWay := way.Tags["oneway"] == "yes" || way.Tags["oneway"] == "1" || way.Tags["oneway"] == "reverse"

	for i := 0; i < len(way.NodeIDs)-1; i++ {
		fromID := way.NodeIDs[i]
		toID := way.NodeIDs[i+1]

		n1, ok1 := meta[fromID]
		n2, ok2 := meta[toID]

		if ok1 && ok2 {
			dist := domain.Haversine(n1.Point, n2.Point)

			graph.Nodes[fromID] = n1
			graph.Nodes[toID] = n2

			edge := &domain.Edge{
				ToID:     toID,
				Distance: dist,
				MaxSpeed: speedMS,
				Weight:   dist / speedMS,
				Lanes:    lanes,
				Highway:  highway,
			}
			graph.Edges[fromID] = append(graph.Edges[fromID], edge)

			if !isOneWay {
				backEdge := *edge
				backEdge.ToID = fromID
				graph.Edges[toID] = append(graph.Edges[toID], &backEdge)
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
