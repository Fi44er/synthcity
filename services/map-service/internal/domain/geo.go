package domain

import "math"

type Coord struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type CoordRes struct {
	Coord  Coord `json:"coord"`
	NodeID int64 `json:"node_id"`
}

func Haversine(c1, c2 Coord) float64 {
	const R = 6371000
	phi1 := c1.Lat * math.Pi / 180
	phi2 := c2.Lat * math.Pi / 180
	dPhi := (c2.Lat - c1.Lat) * math.Pi / 180
	dLambda := (c2.Lon - c1.Lon) * math.Pi / 180

	a := math.Sin(dPhi/2)*math.Sin(dPhi/2) +
		math.Cos(phi1)*math.Cos(phi2)*
			math.Sin(dLambda/2)*math.Sin(dLambda/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
