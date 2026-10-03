package geoconverter

import (
	"math"

	"github.com/tidwall/geodesic"
)

type ILocalGrid interface {
	CellIndexToBounds(cellX, cellY int) GridCell
	GeoToLocal(lat, lon, alt float64) (x, y float64)
	GeoToCellIndex(lat, lon, alt float64) (int, int)
}

type GridCell struct {
	MinLat, MinLon float64 // Минимальные координаты ячейки
	MaxLat, MaxLon float64 // Максимальные координаты ячейки
}

type LocalGrid struct {
	a          float64 // Большая полуось (Equatorial Radius)
	f          float64 // Сжатие (Flattening)
	e2         float64 // Квадрат первого эксцентриситета
	centerLat  float64 // Широта центра
	centerLon  float64 // Долгота центра
	centerAlt  float64 // Высота центра
	cx, cy, cz float64 // Координаты центра в ECEF

	cellSizeM float64 // Размер ячейки в метрах
}

func NewLocalGrid(cellSizeM, centerLat, centerLon, centerAlt float64) ILocalGrid {
	wgs84 := geodesic.WGS84

	g := &LocalGrid{
		a:         wgs84.Radius(),
		f:         wgs84.Flattening(),
		centerLat: centerLat,
		centerLon: centerLon,
		centerAlt: centerAlt,
		cellSizeM: cellSizeM,
	}

	g.e2 = 2*g.f - g.f*g.f

	g.cx, g.cy, g.cz = g.ForwardToECEF(centerLat, centerLon, centerAlt)
	return g
}

// ForwardToECEF преобразует LLA (Lat, Lon, Alt) в ECEF
func (g *LocalGrid) ForwardToECEF(lat, lon, alt float64) (x, y, z float64) {
	latRad := lat * math.Pi / 180.0
	lonRad := lon * math.Pi / 180.0

	sinLat := math.Sin(latRad)
	cosLat := math.Cos(latRad)
	sinLon := math.Sin(lonRad)
	cosLon := math.Cos(lonRad)

	N := g.a / math.Sqrt(1-g.e2*sinLat*sinLat)

	x = (N + alt) * cosLat * cosLon
	y = (N + alt) * cosLat * sinLon
	z = (N*(1-g.e2) + alt) * sinLat
	return
}

// ReverseToLLA преобразует ECEF в LLA
func (g *LocalGrid) ReverseToLLA(x, y, z float64) (lat, lon, alt float64) {
	lon = math.Atan2(y, x) * 180.0 / math.Pi

	p := math.Sqrt(x*x + y*y)
	latRad := math.Atan2(z, p*(1-g.e2))

	for range 5 {
		sinLat := math.Sin(latRad)
		N := g.a / math.Sqrt(1-g.e2*sinLat*sinLat)
		latRad = math.Atan2(z+g.e2*N*sinLat, p)
	}

	lat = latRad * 180.0 / math.Pi
	sinLat := math.Sin(latRad)
	N := g.a / math.Sqrt(1-g.e2*sinLat*sinLat)
	alt = p/math.Cos(latRad) - N
	return
}

// ENUToECEF переводит координаты ENU в координаты ECEF
func (g *LocalGrid) ENUToECEF(east, north, up float64) (x, y, z float64) {
	latRad := g.centerLat * math.Pi / 180.0
	lonRad := g.centerLon * math.Pi / 180.0
	sinLat, cosLat := math.Sincos(latRad)
	sinLon, cosLon := math.Sincos(lonRad)

	x = -sinLon*east - cosLon*sinLat*north + cosLon*cosLat*up + g.cx
	y = cosLon*east - sinLon*sinLat*north + sinLon*cosLat*up + g.cy
	z = cosLat*north + sinLat*up + g.cz
	return
}

// ECEFToENU переводит координаты ECEF в координаты ENU
func (g *LocalGrid) ECEFToENU(x, y, z float64) (east, north, up float64) {
	dx, dy, dz := x-g.cx, y-g.cy, z-g.cz
	latRad := g.centerLat * math.Pi / 180.0
	lonRad := g.centerLon * math.Pi / 180.0
	sinLat, cosLat := math.Sincos(latRad)
	sinLon, cosLon := math.Sincos(lonRad)

	east = -sinLon*dx + cosLon*dy
	north = -cosLon*sinLat*dx - sinLon*sinLat*dy + cosLat*dz
	up = cosLon*cosLat*dx + sinLon*cosLat*dy + sinLat*dz
	return
}

// CellIndexToBounds переводит индекс ячейки в границы ячейки
func (g *LocalGrid) CellIndexToBounds(cellX, cellY int) GridCell {
	eMin, nMin := float64(cellX)*g.cellSizeM, float64(cellY)*g.cellSizeM
	eMax, nMax := float64(cellX+1)*g.cellSizeM, float64(cellY+1)*g.cellSizeM

	xMin, yMin, zMin := g.ENUToECEF(eMin, nMin, 0)
	xMax, yMax, zMax := g.ENUToECEF(eMax, nMax, 0)

	latMin, lonMin, _ := g.ReverseToLLA(xMin, yMin, zMin)
	latMax, lonMax, _ := g.ReverseToLLA(xMax, yMax, zMax)

	return GridCell{MinLat: latMin, MinLon: lonMin, MaxLat: latMax, MaxLon: lonMax}
}

// GeoToCellIndex переводит географические координаты в индекс ячейки
func (g *LocalGrid) GeoToCellIndex(lat, lon, alt float64) (int, int) {
	x, y, z := g.ForwardToECEF(lat, lon, alt)
	east, north, _ := g.ECEFToENU(x, y, z)
	return int(math.Floor(east / g.cellSizeM)), int(math.Floor(north / g.cellSizeM))
}

// GeoToLocal переводит географические координаты в локальные метры (x, y) относительно центра
func (g *LocalGrid) GeoToLocal(lat, lon, alt float64) (x, y float64) {
	ex, ey, ez := g.ForwardToECEF(lat, lon, alt)

	east, north, _ := g.ECEFToENU(ex, ey, ez)

	return east, north
}
