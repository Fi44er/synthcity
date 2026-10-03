package models

import (
	"encoding/json"

	"github.com/Fi44er/synthcity/services/map-service/internal/infrastructure/repository/postgres/types"
)

type StaticObjectModel struct {
	ID       int64     `gorm:"primaryKey;autoIncrement:false"` // OSM ID
	Type     string    `gorm:"index"`                          // "building", "road", "water", "infrastructure"
	Subtype  string    `gorm:"index"`                          // "apartments", "forest", "traffic_light"
	Geometry types.Geo `gorm:"type:geometry(Geometry, 4326);index:,type:gist"`
	// JSONB — это киллер-фича Postgres для таких задач
	Properties json.RawMessage `gorm:"type:jsonb"`
}

func (StaticObjectModel) TableName() string {
	return "static_objects"
}
