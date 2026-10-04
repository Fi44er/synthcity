package domain

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func sampleGraph() *RoadGraph {
	g := NewRoadGraph()
	g.AddNode(&Node{ID: 1, Point: Coord{Lat: 51.000, Lon: 55.000}, Type: NodeRegular})
	g.AddNode(&Node{ID: 2, Point: Coord{Lat: 51.001, Lon: 55.001}, Type: NodeTrafficLight})
	g.AddNode(&Node{ID: 3, Point: Coord{Lat: 51.002, Lon: 55.002}, Type: NodeCrossing})
	g.AddEdge(1, &Edge{ToID: 2, Distance: 100, MaxSpeed: 10, Weight: 10, Lanes: 1, Highway: "residential"})
	g.AddEdge(2, &Edge{ToID: 3, Distance: 100, MaxSpeed: 10, Weight: 10, Lanes: 1, Highway: "residential"})
	return g
}

func writeMsgpack(t *testing.T, path string, v any) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	if err := msgpack.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestCache_RoundTripKeepsNodeTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.bin")

	if err := sampleGraph().SaveBinary(path); err != nil {
		t.Fatalf("SaveBinary: %v", err)
	}
	got, err := LoadBinary(path)
	if err != nil {
		t.Fatalf("LoadBinary: %v", err)
	}

	want := map[int64]NodeType{1: NodeRegular, 2: NodeTrafficLight, 3: NodeCrossing}
	for id, typ := range want {
		n, ok := got.Nodes[id]
		if !ok {
			t.Fatalf("узел %d потерян", id)
		}
		if n.Type != typ {
			t.Errorf("узел %d: Type = %d, want %d", id, n.Type, typ)
		}
	}
	if len(got.Edges[1]) != 1 || got.Edges[1][0].ToID != 2 {
		t.Errorf("рёбра не восстановились: %+v", got.Edges)
	}

	// R-tree должен быть перестроен после загрузки
	id, err := got.GetNearestNode(Coord{Lat: 51.001, Lon: 55.001})
	if err != nil || id != 2 {
		t.Errorf("GetNearestNode = %d, %v; want 2, nil", id, err)
	}
}

func TestCache_LegacyFileWithoutVersionIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.bin")
	// Старый формат: RoadGraph записывался напрямую, без «конверта» и версии.
	writeMsgpack(t, path, sampleGraph())

	if _, err := LoadBinary(path); !errors.Is(err, ErrCacheVersionMismatch) {
		t.Fatalf("err = %v, want ErrCacheVersionMismatch", err)
	}
}

func TestCache_OtherVersionIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.bin")
	writeMsgpack(t, path, &cacheFile{Version: CacheVersion + 1, Graph: sampleGraph()})

	if _, err := LoadBinary(path); !errors.Is(err, ErrCacheVersionMismatch) {
		t.Fatalf("err = %v, want ErrCacheVersionMismatch", err)
	}
}

func TestGetJunctionInfo_ReturnsNodeType(t *testing.T) {
	_, _, typ, err := sampleGraph().GetJunctionInfo(2)
	if err != nil || typ != NodeTrafficLight {
		t.Fatalf("type = %d, err = %v; want NodeTrafficLight", typ, err)
	}
}
