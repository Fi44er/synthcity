package repository

import (
	"context"
	"encoding/json"

	"github.com/Fi44er/synthcity/services/map-service/internal/domain"
	"github.com/Fi44er/synthcity/services/map-service/internal/infrastructure/repository/postgres/models"
	"github.com/Fi44er/synthcity/services/map-service/internal/infrastructure/repository/postgres/types"
	"gorm.io/gorm"
)

type staticRepository struct {
	db *gorm.DB
}

func NewStaticRepository(db *gorm.DB) domain.StaticRepository {
	return &staticRepository{db: db}
}

func (r *staticRepository) BulkSave(ctx context.Context, objects []*domain.StaticObject) error {
	if len(objects) == 0 {
		return nil
	}

	modelsArr := make([]models.StaticObjectModel, 0, len(objects))
	for _, obj := range objects {
		propsJSON, _ := json.Marshal(obj.Properties)

		modelsArr = append(modelsArr, models.StaticObjectModel{
			ID:         obj.ID,
			Type:       string(obj.Type),
			Geometry:   types.NewGeo(obj.Geometry),
			Properties: propsJSON,
		})
	}

	return r.db.WithContext(ctx).
		Save(&modelsArr).Error
}

func (r *staticRepository) GetInBounds(ctx context.Context, minLat, minLon, maxLat, maxLon float64) ([]*domain.StaticObject, error) {
	var models []models.StaticObjectModel

	err := r.db.WithContext(ctx).
		Where("geometry && ST_MakeEnvelope(?, ?, ?, ?, 4326)", minLon, minLat, maxLon, maxLat).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	result := make([]*domain.StaticObject, 0, len(models))
	for _, m := range models {
		var props map[string]any
		json.Unmarshal(m.Properties, &props)

		result = append(result, &domain.StaticObject{
			ID:         m.ID,
			Type:       domain.StaticObjectType(m.Type),
			Geometry:   m.Geometry.Geometry,
			Properties: props,
		})
	}

	return result, nil
}

func (r *staticRepository) ClearLayer(ctx context.Context, objType domain.StaticObjectType) error {
	return r.db.WithContext(ctx).
		Where("type = ?", string(objType)).
		Delete(&models.StaticObjectModel{}).Error
}
