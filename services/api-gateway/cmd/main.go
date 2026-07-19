package main

import (
	"log"

	"github.com/Fi44er/synthcity/services/api-gateway/internal/app"
	"github.com/Fi44er/synthcity/services/api-gateway/internal/config"
)

func main() {
	cfg := config.Load()

	application := app.New(cfg)

	if err := application.Run(); err != nil {
		log.Fatal("stopped", err)
	}
}
