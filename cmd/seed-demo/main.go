package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"mirrorcore/internal/auth"
	"mirrorcore/internal/config"
	"mirrorcore/internal/database"
	"mirrorcore/internal/localdemo"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.LocalDemoMode {
		return fmt.Errorf("refusing to seed local demo data unless LOCAL_DEMO_MODE=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := database.Migrate(ctx, pool); err != nil {
		return err
	}
	if err := auth.NewService(pool).SeedDemoUser(ctx, cfg.BootstrapEmail, cfg.BootstrapPassword); err != nil {
		return err
	}
	videoID, err := localdemo.SeedFixture(ctx, pool, cfg.BootstrapEmail)
	if err != nil {
		return err
	}
	log.Printf("local demo fixture ready: video_id=%s", videoID)
	log.Print("synthetic task_version=2 uses a persisted transcript checkpoint; cmd/background continues it without resetting existing progress")
	log.Print("example question: 这个视频里 Kafka 和执行 epoch 分别起什么作用？")
	return nil
}
