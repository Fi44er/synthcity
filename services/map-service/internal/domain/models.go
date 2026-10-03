package domain

type NodeType int8

const (
	NodeUnspecified NodeType = iota
	NodeRegular
	NodeTrafficLight
	NodeCrossing
)

type Node struct {
	ID    int64
	Point Coord
	Type  NodeType
}

type Edge struct {
	ToID     int64
	Distance float64
	MaxSpeed float64
	Weight   float64
	Lanes    int
	Highway  string
}

type Route struct {
	Points   []Coord
	NodeIDs  []int64
	Distance float64
	Duration float64
}

type JunctionInfo struct {
	ID               int64
	Type             NodeType
	ConnectedNodeIDs []int64
}
