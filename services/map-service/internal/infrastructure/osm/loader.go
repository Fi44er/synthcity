package osm

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/Fi44er/synthcity/pkg/logger"
	"github.com/Fi44er/synthcity/pkg/telemetry"
	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"go.uber.org/zap"
)

type ILoader interface {
	LoadGraph(ctx context.Context) (*domain.RoadGraph, error)
}

type Loader struct {
	graphProc  *GraphProcessor
	staticProc *StaticProcessor
	osmPath    string
}

func NewLoader(osmPath string, graphProc *GraphProcessor, staticProc *StaticProcessor) ILoader {
	return &Loader{osmPath: osmPath, graphProc: graphProc, staticProc: staticProc}
}

func (l *Loader) LoadGraph(ctx context.Context) (*domain.RoadGraph, error) {
	pbfPath := l.osmPath
	start := time.Now()

	log := logger.FromContext(ctx).With(
		zap.String("operation", "LoadGraph"),
		zap.String("pbf_path", pbfPath),
	)

	log.Info("starting graph loading")

	file, err := os.Open(pbfPath)
	if err != nil {
		log.Error("failed to open pbf file", zap.Error(err))
		return nil, fmt.Errorf("open pbf: %w", err)
	}
	defer file.Close()

	scanner := osmpbf.New(context.Background(), file, runtime.GOMAXPROCS(-1))
	defer scanner.Close()

	allNodeMeta := make(map[int64]*domain.Node)
	var rawNodes, rawWays, rawRelations int64

	for scanner.Scan() {
		switch o := scanner.Object().(type) {
		case *osm.Node:
			rawNodes++
			node := &domain.Node{
				ID:    int64(o.ID),
				Point: domain.Coord{Lat: o.Lat, Lon: o.Lon},
			}
			allNodeMeta[int64(o.ID)] = node

			l.staticProc.ProcessNode(o)

		case *osm.Way:
			rawWays++
			l.graphProc.ProcessWay(o, allNodeMeta)
			l.staticProc.ProcessWay(o, allNodeMeta)

			if (rawNodes+rawWays)%10000 == 0 {
				log.Debug("processing progress",
					zap.Int64("nodes_read", rawNodes),
					zap.Int64("ways_read", rawWays),
				)
			}

		case *osm.Relation:
			rawRelations++
			l.staticProc.ProcessRelation(o, allNodeMeta)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan pbf: %w", err)
	}

	telemetry.RecordMetrics(ctx, start, "map-service", "LoadGraph", "success")

	return l.graphProc.graph, nil
}
