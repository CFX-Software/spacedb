package config

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

type Config struct {
	Listen    string          `json:"listen"`
	Transport TransportConfig `json:"transport"`
	Database  DatabaseConfig  `json:"database"`
	Realtime  RealtimeConfig  `json:"realtime"`
}

type TransportConfig struct {
	Listen string `json:"listen"`
}

type DatabaseConfig struct {
	Driver                 string `json:"driver"`
	DSN                    string `json:"dsn"`
	MaxOpenConns           int    `json:"maxOpenConns"`
	MaxIdleConns           int    `json:"maxIdleConns"`
	ConnMaxLifetimeSeconds int    `json:"connMaxLifetimeSeconds"`
	QueryTimeoutMs         int    `json:"queryTimeoutMs"`
	SlowQueryMs            int    `json:"slowQueryMs"`
}

type RealtimeConfig struct {
	Enabled        bool `json:"enabled"`
	PollIntervalMs int  `json:"pollIntervalMs"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	cfg.Defaults()
	return cfg, cfg.Validate()
}

func (c *Config) Defaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:37120"
	}
	if c.Transport.Listen == "" {
		c.Transport.Listen = "127.0.0.1:37121"
	}
	if c.Database.MaxOpenConns == 0 {
		c.Database.MaxOpenConns = 32
	}
	if c.Database.MaxIdleConns == 0 {
		c.Database.MaxIdleConns = 16
	}
	if c.Database.ConnMaxLifetimeSeconds == 0 {
		c.Database.ConnMaxLifetimeSeconds = 1800
	}
	if c.Database.QueryTimeoutMs == 0 {
		c.Database.QueryTimeoutMs = 5000
	}
	if c.Database.SlowQueryMs == 0 {
		c.Database.SlowQueryMs = 100
	}
	if c.Realtime.PollIntervalMs == 0 {
		c.Realtime.PollIntervalMs = 250
	}
}

func (c Config) Validate() error {
	if c.Database.Driver == "" {
		return errors.New("database.driver is required")
	}
	if c.Database.DSN == "" {
		return errors.New("database.dsn is required")
	}
	switch c.Database.Driver {
	case "postgres", "mysql", "mariadb":
		return nil
	default:
		return errors.New("database.driver must be postgres, mysql, or mariadb")
	}
}

func (c Config) QueryTimeout() time.Duration {
	return time.Duration(c.Database.QueryTimeoutMs) * time.Millisecond
}

func (c Config) SlowQueryThreshold() time.Duration {
	return time.Duration(c.Database.SlowQueryMs) * time.Millisecond
}

func (c Config) PollInterval() time.Duration {
	return time.Duration(c.Realtime.PollIntervalMs) * time.Millisecond
}
