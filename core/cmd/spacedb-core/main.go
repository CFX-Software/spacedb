package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/inkwell/spacedb/core/internal/config"
	"github.com/inkwell/spacedb/core/internal/server"
)

func main() {
	configPath := flag.String("config", "config.json", "path to spacedb config")
	checkConfig := flag.Bool("check-config", false, "validate config and exit")
	healthURL := flag.String("health", "", "check a running spacedb core health endpoint and exit")
	flag.Parse()

	if *healthURL != "" {
		if err := health(*healthURL); err != nil {
			slog.Error("health check failed", "error", err)
			os.Exit(1)
		}
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	if *checkConfig {
		fmt.Println("spacedb config ok")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := server.New(cfg)
	if err != nil {
		slog.Error("create server", "error", err)
		os.Exit(1)
	}

	if err := srv.Run(ctx); err != nil {
		slog.Error("run server", "error", err)
		os.Exit(1)
	}
}

func health(url string) error {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	out, _ := json.Marshal(payload)
	fmt.Println(string(out))
	return nil
}
