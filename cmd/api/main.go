package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/application"
	"mirrorcore/internal/auth"
	"mirrorcore/internal/config"
	"mirrorcore/internal/content"
	"mirrorcore/internal/database"
	"mirrorcore/internal/httpapi"
	"mirrorcore/internal/localdemo"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/providerquota"
	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/qwen"
	"mirrorcore/internal/qa"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer startupCancel()
	pool, err := database.Open(startupCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := database.Migrate(startupCtx, pool); err != nil {
		return err
	}
	apiMetrics := observability.New("api")

	var uploadService *application.UploadService
	var objectStore *ossstore.Client
	if cfg.ObjectStorage.Enabled() {
		objectStore, err = ossstore.New(cfg.ObjectStorage)
		if err != nil {
			return err
		}
		uploadService = application.NewUploadService(postgres.NewUploadRepository(pool, cfg.ObjectStorage.Bucket), objectStore)
	}
	var queryEmbedder qa.Embedder
	var answerGenerator qa.AnswerGenerator
	var contentGenerator content.Generator
	if cfg.LocalDemoMode {
		queryEmbedder = localdemo.Embedder{}
		answerGenerator = localdemo.Generator{}
		contentGenerator = localdemo.ContentGenerator{}
		logger.Warn("LOCAL_DEMO_MODE enabled: deterministic fixture providers are active; no cloud calls will be made")
	} else if cfg.Bailian.EmbeddingEnabled() {
		embeddingLimiter, limiterErr := providerquota.New(startupCtx, pool, cfg.Bailian.WorkspaceID,
			embedding.Model, cfg.Bailian.EmbeddingRPM, cfg.Bailian.EmbeddingTPM, apiMetrics)
		if limiterErr != nil {
			return limiterErr
		}
		embeddingClient, clientErr := embedding.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
		if clientErr != nil {
			return clientErr
		}
		queryEmbedder, err = providerquota.NewEmbeddingAdapter(embeddingClient, embeddingLimiter, apiMetrics)
		if err != nil {
			return err
		}
		qwenLimiter, limiterErr := providerquota.New(startupCtx, pool, cfg.Bailian.WorkspaceID,
			qwen.Model, cfg.Bailian.QwenRPM, cfg.Bailian.QwenTPM, apiMetrics)
		if limiterErr != nil {
			return limiterErr
		}
		qwenClient, clientErr := qwen.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
		if clientErr != nil {
			return clientErr
		}
		qwenGenerator, generatorErr := providerquota.NewQwenAdapter(qwenClient, qwenLimiter, apiMetrics)
		if generatorErr != nil {
			return generatorErr
		}
		contentGenerator = qwenGenerator
		answerGenerator = qwenGenerator
	} else {
		logger.Warn("question model providers disabled: BAILIAN_WORKSPACE_ID and BAILIAN_API_KEY are not configured")
	}
	var questionService *qa.Service
	if cfg.LocalDemoMode {
		questionService, err = qa.NewLocalDemoService(postgres.NewQuestionRepository(pool), queryEmbedder,
			answerGenerator, uuid.NewString())
	} else {
		questionService, err = qa.NewService(postgres.NewQuestionRepository(pool), queryEmbedder, answerGenerator,
			cfg.Bailian.WorkspaceID, uuid.NewString())
	}
	if err != nil {
		return err
	}
	var contentService *content.Service
	if cfg.LocalDemoMode {
		contentService, err = content.NewLocalDemoService(postgres.NewContentRepository(pool), contentGenerator)
	} else {
		contentService, err = content.NewService(postgres.NewContentRepository(pool), contentGenerator, cfg.Bailian.WorkspaceID)
	}
	if err != nil {
		return err
	}
	runtime := httpapi.NewRuntimeWithPlaybackAndMetrics(pool, logger, cfg.CookieSecure, uploadService, questionService,
		objectStore, cfg.ObjectStorage.Bucket, apiMetrics, contentService)
	if err := auth.NewService(pool).SeedDemoUser(startupCtx, cfg.BootstrapEmail, cfg.BootstrapPassword); err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           runtime,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      210 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsServer, metricsListener, err := observability.NewHTTPServer(cfg.APIMetricsAddr, apiMetrics)
	if err != nil {
		return errors.New("start API metrics listener: " + err.Error())
	}
	defer metricsListener.Close()
	maintenanceCtx, stopMaintenance := context.WithCancel(context.Background())
	maintenanceDone := make(chan struct{})
	var maintenanceWG sync.WaitGroup
	// Keep a global pruner alive even if a provider is currently disabled so
	// ledger rows for removed workspaces/models can still expire.
	maintenanceWG.Add(1)
	go func() {
		defer maintenanceWG.Done()
		providerquota.RunLedgerPruner(maintenanceCtx, pool, time.Minute, func(err error) {
			apiMetrics.ObserveProviderQuotaPruneFailure()
			logger.Warn("provider quota reservation pruning failed", "error", err)
		})
	}()
	maintenanceWG.Add(1)
	go func() {
		defer maintenanceWG.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			maintenanceCtxCall, cancel := context.WithTimeout(maintenanceCtx, 3*time.Second)
			reaped, reapErr := questionService.ReapExpired(maintenanceCtxCall)
			cancel()
			if reapErr != nil {
				logger.Warn("question lease maintenance failed", "error", reapErr)
			} else if reaped > 0 {
				logger.Info("expired question executions reaped", "count", reaped)
			}
			select {
			case <-maintenanceCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	go func() {
		maintenanceWG.Wait()
		close(maintenanceDone)
	}()
	defer func() {
		stopMaintenance()
		<-maintenanceDone
	}()
	serverErr := make(chan error, 1)
	metricsErr := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		serverErr <- server.ListenAndServe()
	}()
	go func() {
		logger.Info("API metrics listening", "addr", cfg.APIMetricsAddr)
		metricsErr <- metricsServer.Serve(metricsListener)
	}()

	shutdownCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case err := <-metricsErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("API metrics server stopped unexpectedly: " + err.Error())
		}
	case <-shutdownCtx.Done():
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runtime.StopQuestionExecutions()
	if err := server.Shutdown(ctx); err != nil {
		return err
	}
	if err := metricsServer.Shutdown(ctx); err != nil {
		return err
	}
	stopMaintenance()
	<-maintenanceDone
	logger.Info("api shutdown complete")
	return nil
}
