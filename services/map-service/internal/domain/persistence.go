package domain

import (
	"bufio"
	"os"

	"github.com/dhconnelly/rtreego"
	"github.com/vmihailenco/msgpack/v5"
)

func (g *RoadGraph) SaveBinary(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	enc := msgpack.NewEncoder(writer)

	err = enc.Encode(g)
	if err != nil {
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

	g := NewRoadGraph()

	reader := bufio.NewReader(file)

	dec := msgpack.NewDecoder(reader)
	if err := dec.Decode(g); err != nil {
		return nil, err
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
