package domain

import (
	"bufio"
	"errors"
	"fmt"
	"os"

	"github.com/dhconnelly/rtreego"
	"github.com/vmihailenco/msgpack/v5"
)

const CacheVersion = 1

var ErrCacheVersionMismatch = errors.New("graph cache version mismatch")

type cacheFile struct {
	Version int        `msgpack:"version"`
	Graph   *RoadGraph `msgpack:"graph"`
}

func (g *RoadGraph) SaveBinary(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	enc := msgpack.NewEncoder(writer)

	if err := enc.Encode(&cacheFile{Version: CacheVersion, Graph: g}); err != nil {
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

	var snap cacheFile
	if err := msgpack.NewDecoder(bufio.NewReader(file)).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decode graph cache: %w", err)
	}

	// Старые кэши (без поля version) декодируются с Version == 0 и тоже отвергаются.
	if snap.Version != CacheVersion {
		return nil, fmt.Errorf("%w: в файле %d, ожидается %d", ErrCacheVersionMismatch, snap.Version, CacheVersion)
	}
	if snap.Graph == nil {
		return nil, errors.New("graph cache is empty")
	}

	g := snap.Graph
	if g.Nodes == nil {
		g.Nodes = make(map[int64]*Node)
	}
	if g.Edges == nil {
		g.Edges = make(map[int64][]*Edge)
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
