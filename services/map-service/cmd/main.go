package main

import (
	"log"

	"github.com/Fi44er/synthcity/services/map-service/internal/app"
	"github.com/Fi44er/synthcity/services/map-service/internal/config"
)

func main() {
	cfg := config.Load()
	application := app.New(cfg)

	if err := application.Run(); err != nil {
		log.Fatal("map-service stopped", err)
	}
}
