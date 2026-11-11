package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"mirrorcore/internal/background/asrreconcile"
	"mirrorcore/internal/background/contentworker"
	"mirrorcore/internal/background/deletion"
	"mirrorcore/internal/background/inbox"
	"mirrorcore/internal/background/mediaworker"
	"mirrorcore/internal/background/outbox"
	"mirrorcore/internal/background/tasks"
	"mirrorcore/internal/background/uploads"
	"mirrorcore/internal/config"
	"mirrorcore/internal/content"
	"mirrorcore/internal/database"
	"mirrorcore/internal/localdemo"
	"mirrorcore/internal/observability"
	"mirrorcore/internal/providerquota"
	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/funasr"
	"mirrorcore/internal/providers/qwen"
	"mirrorcore/internal/queue/kafka"
	"mirrorcore/internal/repository/postgres"
	"mirrorcore/internal/storage/ossstore"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("background stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	startupCtx, startupCancel := context.WithTimeout(ctx, 15*time.Second)
	defer startupCancel()
	pool, err := database.Open(startupCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := database.Migrate(startupCtx, pool); err != nil {
		return err
	}
	backgroundMetrics := observability.New("background")
	backgroundMetrics.AttachPool(pool)
	backgroundMetrics.AttachQueueStats(pool)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := kafka.EnsureTopics(startupCtx, cfg.KafkaBrokers); err != nil {
		return err
	}
	kafkaLagReader, err := observability.NewKafkaConsumerLagReader(producer, kafka.ParseConsumerGroup, kafka.ParseRequestedTopic)
	if err != nil {
		return err
	}
	backgroundMetrics.AttachKafkaConsumerLag(kafkaLagReader)
	relay := outbox.New(pool, outbox.NewKgoSender(producer), logger)
	consumer := inbox.NewConsumer(cfg.KafkaBrokers, inbox.NewProcessor(pool), logger)
	taskManager := tasks.NewManager(pool, logger)
	var uploadManager *uploads.Manager
	var asrReconciler *asrreconcile.Worker
	var mediaProcessor *mediaworker.Processor
	var asrProcessor *mediaworker.ASRProcessor
	var transcriptProcessor *mediaworker.TranscriptProcessor
	var embeddingProcessor *mediaworker.EmbeddingProcessor
	var contentWorker *contentworker.Worker
	var deletionWorker *deletion.Worker
	chunkProcessor, err := mediaworker.NewChunkProcessor(postgres.NewStageRepository(pool))
	if err != nil {
		return err
	}
	publishWorkspaceID := cfg.Bailian.WorkspaceID
	if cfg.LocalDemoMode {
		publishWorkspaceID = "local-demo"
	}
	publishProcessor, err := mediaworker.NewPublishProcessor(postgres.NewStageRepository(pool), publishWorkspaceID)
	if err != nil {
		return err
	}
	var objectStore *ossstore.Client
	var artifactStore mediaworker.ArtifactStore
	artifactBucket := cfg.ObjectStorage.Bucket
	if cfg.ObjectStorage.Enabled() {
		objectStore, err = ossstore.New(cfg.ObjectStorage)
		if err != nil {
			return err
		}
		artifactStore = objectStore
		deletionWorker = deletion.NewWorker(postgres.NewVideoDeletionRepository(pool), objectStore,
			cfg.ObjectStorage.Bucket, logger, backgroundMetrics)
		uploadManager = uploads.NewManager(postgres.NewUploadRepository(pool, cfg.ObjectStorage.Bucket), objectStore, logger)
		transcriptProcessor, err = mediaworker.NewTranscriptProcessor(postgres.NewStageRepository(pool), artifactStore,
			cfg.ObjectStorage.Bucket)
		if err != nil {
			return err
		}
		if _, ffmpegErr := exec.LookPath(cfg.MediaTools.FFmpegPath); ffmpegErr != nil {
			logger.Warn("media worker disabled: ffmpeg not found", "configured_path", cfg.MediaTools.FFmpegPath)
		} else if _, ffprobeErr := exec.LookPath(cfg.MediaTools.FFprobePath); ffprobeErr != nil {
			logger.Warn("media worker disabled: ffprobe not found", "configured_path", cfg.MediaTools.FFprobePath)
		} else {
			mediaProcessor, err = mediaworker.NewProcessor(postgres.NewStageRepository(pool), artifactStore,
				cfg.MediaTools, cfg.ObjectStorage.Bucket)
			if err != nil {
				return err
			}
		}
	} else if cfg.LocalDemoMode {
		artifactStore, err = localdemo.NewFileObjectStore(filepath.Join(cfg.MediaTools.TempDir, "local-demo-objects"))
		if err != nil {
			return err
		}
		artifactBucket = localdemo.LocalDemoBucket
		transcriptProcessor, err = mediaworker.NewTranscriptProcessor(postgres.NewStageRepository(pool), artifactStore, artifactBucket)
		if err != nil {
			return err
		}
		if _, ffmpegErr := exec.LookPath(cfg.MediaTools.FFmpegPath); ffmpegErr != nil {
			logger.Warn("local demo media stages disabled: ffmpeg wrapper/tool is unavailable", "configured_path", cfg.MediaTools.FFmpegPath)
		} else if _, ffprobeErr := exec.LookPath(cfg.MediaTools.FFprobePath); ffprobeErr != nil {
			logger.Warn("local demo media stages disabled: ffprobe wrapper/tool is unavailable", "configured_path", cfg.MediaTools.FFprobePath)
		} else {
			mediaProcessor, err = mediaworker.NewProcessor(postgres.NewStageRepository(pool), artifactStore,
				cfg.MediaTools, artifactBucket)
			if err != nil {
				return err
			}
		}
		identity := mediaworker.ASRIdentity{Profile: "local_demo_asr_v1", Provider: "local-demo",
			Region: "local", Workspace: "local-demo", Model: "deterministic-asr-v1"}
		asrProcessor, err = mediaworker.NewASRProcessorWithIdentity(postgres.NewStageRepository(pool), artifactStore,
			localdemo.ASRProvider{}, artifactBucket, identity, cfg.MediaTools.TempDir)
		if err != nil {
			return err
		}
		logger.Warn("LOCAL_DEMO_MODE enabled: media artifacts use the local filesystem and ASR returns a fixed synthetic transcript; no cloud calls will be made")
	} else {
		logger.Warn("media worker disabled: OSS is not configured")
	}
	if cfg.LocalDemoMode {
		logger.Info("local deterministic ASR adapter enabled with isolated provider identity")
	} else if cfg.Bailian.ASREnabled() {
		provider, providerErr := funasr.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
		if providerErr != nil {
			return providerErr
		}
		asrReconciler, err = asrreconcile.NewWorker(postgres.NewASRReconciliationRepository(pool),
			provider, cfg.Bailian.Region, cfg.Bailian.WorkspaceID, logger)
		if err != nil {
			return err
		}
		if objectStore == nil {
			logger.Warn("Fun-ASR processing disabled: OSS is not configured; remote task reconciliation remains enabled")
		} else {
			asrProcessor, err = mediaworker.NewASRProcessor(postgres.NewStageRepository(pool), objectStore,
				provider, cfg.ObjectStorage.Bucket, cfg.Bailian.Region, cfg.Bailian.WorkspaceID, cfg.MediaTools.TempDir)
			if err != nil {
				return err
			}
		}
	} else {
		logger.Info("Fun-ASR stage disabled: BAILIAN_WORKSPACE_ID and BAILIAN_API_KEY are not configured")
	}
	if cfg.LocalDemoMode {
		identity := mediaworker.EmbeddingIdentity{Profile: "local_demo_embedding_v1", Provider: "local-demo",
			Region: "local", WorkspaceID: "local-demo", Model: "deterministic-unit-vector-v1",
			Dimension: embedding.Dimension, TextType: "document", OutputType: "dense",
			BatchSize: embedding.MaxBatch, MaxBatchAttempts: 3}
		embeddingProcessor, err = mediaworker.NewEmbeddingProcessorWithIdentity(postgres.NewStageRepository(pool),
			localdemo.Embedder{}, identity)
		if err != nil {
			return err
		}
		logger.Warn("LOCAL_DEMO_MODE enabled: deterministic document embeddings are active; no cloud calls will be made")
	} else if cfg.Bailian.EmbeddingEnabled() {
		embeddingLimiter, providerErr := providerquota.New(startupCtx, pool, cfg.Bailian.WorkspaceID,
			embedding.Model, cfg.Bailian.EmbeddingRPM, cfg.Bailian.EmbeddingTPM, backgroundMetrics)
		if providerErr != nil {
			return providerErr
		}
		embeddingClient, providerErr := embedding.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
		if providerErr != nil {
			return providerErr
		}
		provider, providerErr := providerquota.NewEmbeddingAdapter(embeddingClient, embeddingLimiter, backgroundMetrics)
		if providerErr != nil {
			return providerErr
		}
		embeddingProcessor, err = mediaworker.NewEmbeddingProcessor(postgres.NewStageRepository(pool), provider,
			cfg.Bailian.Region, cfg.Bailian.WorkspaceID)
		if err != nil {
			return err
		}
	} else {
		logger.Info("Embedding provider unavailable: BAILIAN_WORKSPACE_ID and BAILIAN_API_KEY are not configured")
		embeddingProcessor, err = mediaworker.NewEmbeddingProcessor(postgres.NewStageRepository(pool), nil,
			cfg.Bailian.Region, "")
		if err != nil {
			return err
		}
	}
	if cfg.LocalDemoMode {
		contentService, serviceErr := content.NewLocalDemoService(postgres.NewContentRepository(pool),
			localdemo.ContentGenerator{})
		if serviceErr != nil {
			return serviceErr
		}
		contentWorker = contentworker.NewWorker(contentService, logger, backgroundMetrics)
		logger.Warn("LOCAL_DEMO_MODE enabled: deterministic content fixtures are active; no cloud calls will be made")
	} else if cfg.Bailian.EmbeddingEnabled() {
		qwenLimiter, providerErr := providerquota.New(startupCtx, pool, cfg.Bailian.WorkspaceID,
			qwen.Model, cfg.Bailian.QwenRPM, cfg.Bailian.QwenTPM, backgroundMetrics)
		if providerErr != nil {
			return providerErr
		}
		qwenClient, providerErr := qwen.New(cfg.Bailian.WorkspaceID, cfg.Bailian.APIKey)
		if providerErr != nil {
			return providerErr
		}
		generator, providerErr := providerquota.NewQwenAdapter(qwenClient, qwenLimiter, backgroundMetrics)
		if providerErr != nil {
			return providerErr
		}
		contentService, serviceErr := content.NewService(postgres.NewContentRepository(pool), generator, cfg.Bailian.WorkspaceID)
		if serviceErr != nil {
			return serviceErr
		}
		contentWorker = contentworker.NewWorker(contentService, logger, backgroundMetrics)
	} else {
		logger.Info("content worker disabled: BAILIAN_WORKSPACE_ID and BAILIAN_API_KEY are not configured")
	}
	processors := make(map[string]mediaworker.StageProcessor, 5)
	if mediaProcessor != nil {
		processors["PROBE"] = mediaProcessor
		processors["AUDIO"] = mediaProcessor
	}
	if asrProcessor != nil {
		processors["ASR"] = asrProcessor
	}
	if transcriptProcessor != nil {
		processors["TRANSCRIPT"] = transcriptProcessor
	}
	processors["CHUNK"] = chunkProcessor
	processors["PUBLISH"] = publishProcessor
	if embeddingProcessor != nil {
		processors["EMBEDDING"] = embeddingProcessor
	}

	type componentResult struct {
		name string
		err  error
	}
	metricsServer, metricsListener, err := observability.NewHTTPServer(cfg.BackgroundMetricsAddr, backgroundMetrics)
	if err != nil {
		return errors.New("start background metrics listener: " + err.Error())
	}
	defer metricsListener.Close()
	stopMetrics := func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := metricsServer.Shutdown(stopCtx); err != nil {
			logger.Warn("background metrics shutdown failed", "error", err)
		}
	}
	componentCount := 5 // includes the process-wide provider quota ledger pruner
	if uploadManager != nil {
		componentCount++
	}
	if asrReconciler != nil {
		componentCount++
	}
	if len(processors) > 0 {
		componentCount++
	}
	if contentWorker != nil {
		componentCount++
	}
	if deletionWorker != nil {
		componentCount++
	}
	results := make(chan componentResult, componentCount)
	go func() { results <- componentResult{name: "outbox", err: relay.Run(loopCtx)} }()
	go func() { results <- componentResult{name: "inbox", err: consumer.Run(loopCtx)} }()
	go func() { results <- componentResult{name: "task-maintenance", err: taskManager.RunMaintenance(loopCtx)} }()
	go func() {
		logger.Info("background metrics listening", "addr", cfg.BackgroundMetricsAddr)
		results <- componentResult{name: "metrics", err: metricsServer.Serve(metricsListener)}
	}()
	if uploadManager != nil {
		go func() {
			results <- componentResult{name: "upload-maintenance", err: uploadManager.RunMaintenance(loopCtx)}
		}()
	}
	if asrReconciler != nil {
		go func() { results <- componentResult{name: "asr-reconciler", err: asrReconciler.Run(loopCtx)} }()
	}
	if len(processors) > 0 {
		worker := mediaworker.NewWorker(taskManager, postgres.NewStageRepository(pool), processors,
			cfg.MediaTools.TempDir, logger, backgroundMetrics)
		go func() { results <- componentResult{name: "media-worker", err: worker.Run(loopCtx)} }()
	}
	if contentWorker != nil {
		go func() { results <- componentResult{name: "content-worker", err: contentWorker.Run(loopCtx)} }()
	}
	if deletionWorker != nil {
		go func() { results <- componentResult{name: "video-deletion-worker", err: deletionWorker.Run(loopCtx)} }()
	}
	go func() {
		providerquota.RunLedgerPruner(loopCtx, pool, time.Minute, func(err error) {
			backgroundMetrics.ObserveProviderQuotaPruneFailure()
			logger.Warn("provider quota reservation pruning failed", "error", err)
		})
		results <- componentResult{name: "provider-quota-ledger-pruner"}
	}()
	logger.Info("background loops started", "kafka_brokers", cfg.KafkaBrokers)

	select {
	case <-ctx.Done():
		logger.Info("background shutting down")
		cancelLoops()
		stopMetrics()
		for i := 0; i < componentCount; i++ {
			<-results
		}
		return nil
	case result := <-results:
		cancelLoops()
		stopMetrics()
		for i := 0; i < componentCount-1; i++ {
			other := <-results
			if result.err == nil || errors.Is(result.err, context.Canceled) {
				result = other
			}
		}
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			return result.err
		}
		return nil
	}
}
