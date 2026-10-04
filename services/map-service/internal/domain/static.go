package domain

import (
	"context"

	"github.com/paulmach/orb"
)

type StaticObjectType string

const (
	TypeBuilding       StaticObjectType = "building"
	TypeGreenZone      StaticObjectType = "green_zone"
	TypeWater          StaticObjectType = "water"
	TypeInfrastructure StaticObjectType = "infrastructure"
	TypeTrafficLight   StaticObjectType = "traffic_light"
	TypeCrossing       StaticObjectType = "crossing"
)

// StaticObject — универсальная модель для зданий, парков и т.д.
type StaticObject struct {
	ID         int64            // Оригинальный OSM ID
	Type       StaticObjectType // Категория
	Subtype    string           // Подкатегория
	Geometry   orb.Geometry     // Геометрия (точка, полигон или мультиполигон)
	Properties map[string]any   // Метаданные: этажность, название, тип покрытия
}

type StaticRepository interface {
	BulkSave(ctx context.Context, objects []*StaticObject) error
	GetInBounds(ctx context.Context, minLat, minLon, maxLat, maxLon float64) ([]*StaticObject, error)
	ClearLayer(ctx context.Context, objType StaticObjectType) error
}
