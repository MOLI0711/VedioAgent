package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"mirrorcore/internal/auth"
	"mirrorcore/internal/config"
	"mirrorcore/internal/database"
	"mirrorcore/internal/localdemo"
)

func main() {
	fixtureKey := flag.String("fixture-key", "media-v1", "stable identity for an isolated synthetic media fixture")
	flag.Parse()
	if err := run(*fixtureKey); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(fixtureKey string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.LocalDemoMode {
		return fmt.Errorf("refusing to seed a local media fixture unless LOCAL_DEMO_MODE=true")
	}
	fixturePath := os.Getenv("LOCAL_DEMO_MEDIA_PATH")
	if fixturePath == "" {
		fixturePath = filepath.Join("assets", "localdemo", "voice-demo.mp4")
	}
	store, err := localdemo.NewFileObjectStore(filepath.Join(cfg.MediaTools.TempDir, "local-demo-objects"))
	if err != nil {
		return err
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
	videoID, taskID, err := localdemo.SeedMediaFixtureWithKey(ctx, pool, store,
		cfg.BootstrapEmail, fixturePath, fixtureKey)
	if err != nil {
		return err
	}
	log.Printf("synthetic local media fixture ready: fixture_key=%s video_id=%s task_id=%s task_version=1",
		fixtureKey, videoID, taskID)
	log.Print("the MP4 is synthetic; LOCAL_DEMO_MODE uses a deterministic transcript adapter and makes no cloud calls")
	log.Print("start cmd/background with scripts/localdemo/ffmpeg and scripts/localdemo/ffprobe to run PROBE through PUBLISH")
	return nil
}
