package config

import (
	"log"

	"github.com/caarlos0/env/v10" // рекомендуемая библиотека для конфигов
	"github.com/joho/godotenv"
)

type Config struct {
	GRPCPort         string `env:"MAP_GRPC_PORT" envDefault:"50051"`
	AppEnv           string `env:"APP_ENV" envDefault:"production"`
	OtelCollectorURL string `env:"OTEL_COLLECTOR_URL" envDefault:"localhost:4318"`
	PbfPath          string `env:"MAP_PBF_PATH" envDefault:"./internal/data/test.pbf"`
}

func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}
	return cfg
}
