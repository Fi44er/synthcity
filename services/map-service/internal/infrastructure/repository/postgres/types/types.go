package types

import (
	"database/sql/driver"
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/ewkb"
)

type Geo struct {
	orb.Geometry
}

func NewGeo(geom orb.Geometry) Geo {
	return Geo{Geometry: geom}
}

// Value реализует интерфейс driver.Valuer (конвертация для записи в БД)
func (g Geo) Value() (driver.Value, error) {
	if g.Geometry == nil {
		return nil, nil
	}
	// Конвертируем orb.Geometry в формат EWKB (Extended Well-Known Binary) с SRID 4326 (WGS84)
	return ewkb.Marshal(g.Geometry, 4326)
}

// Scan реализует интерфейс sql.Scanner (чтение из БД в структуру Go)
func (g *Geo) Scan(value any) error {
	if value == nil {
		return nil
	}

	buf, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("invalid type for Geo Scan: %T", value)
	}

	// Декодируем из EWKB в orb.Geometry
	geom, _, err := ewkb.Unmarshal(buf)
	if err != nil {
		return err
	}

	g.Geometry = geom
	return nil
}

// GormDataType указывает GORM использовать тип geometry в Postgres
func (Geo) GormDataType() string {
	return "geometry(GEOMETRY, 4326)"
}
