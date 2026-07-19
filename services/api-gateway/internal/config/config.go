package config

import (
	"log"
	"time"

	"github.com/caarlos0/env/v10" // рекомендуемая библиотека для конфигов
	"github.com/joho/godotenv"
)

type Config struct {
	AppPort          string `env:"APP_PORT" envDefault:"50052"`
	Environment      string `env:"APP_ENV" envDefault:"production"`
	OtelCollectorURL string `env:"OTEL_COLLECTOR_URL" envDefault:"localhost:4318"`

	HelloServiceAddr string `env:"HELLO_SVC_ADDR" envDefault:"hi-service:50051"`
	MapServiceAddr   string `env:"MAP_SVC_ADDR" envDefault:"map-service:50054"`

	ReadTimeout  time.Duration `env:"READ_TIMEOUT" envDefault:"5s"`
	WriteTimeout time.Duration `env:"WRITE_TIMEOUT" envDefault:"10s"`
	IdleTimeout  time.Duration `env:"IDLE_TIMEOUT" envDefault:"120s"`
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
