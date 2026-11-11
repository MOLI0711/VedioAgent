package config

import (
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr              string
	APIMetricsAddr        string
	BackgroundMetricsAddr string
	DatabaseURL           string
	KafkaBrokers          []string
	ObjectStorage         ObjectStorage
	MediaTools            MediaTools
	Bailian               Bailian
	CookieSecure          bool
	LocalDemoMode         bool
	BootstrapEmail        string
	BootstrapPassword     string
}

type MediaTools struct {
	FFmpegPath  string
	FFprobePath string
	TempDir     string
}

type Bailian struct {
	Region       string
	WorkspaceID  string
	APIKey       string
	EmbeddingRPM int
	EmbeddingTPM int64
	QwenRPM      int
	QwenTPM      int64
}

func (b Bailian) ASREnabled() bool       { return b.WorkspaceID != "" && b.APIKey != "" }
func (b Bailian) EmbeddingEnabled() bool { return b.WorkspaceID != "" && b.APIKey != "" }

type ObjectStorage struct {
	Region         string
	Endpoint       string
	Bucket         string
	CredentialMode string
}

func (c ObjectStorage) Enabled() bool { return c.Bucket != "" }

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:              envOr("HTTP_ADDR", ":8080"),
		APIMetricsAddr:        envOr("API_METRICS_ADDR", "127.0.0.1:9091"),
		BackgroundMetricsAddr: envOr("BACKGROUND_METRICS_ADDR", "127.0.0.1:9093"),
		DatabaseURL:           strings.TrimSpace(os.Getenv("DATABASE_URL")),
		KafkaBrokers:          splitList(envOr("KAFKA_BROKERS", "localhost:9092")),
		ObjectStorage: ObjectStorage{
			Region:         strings.TrimSpace(os.Getenv("OSS_REGION")),
			Endpoint:       strings.TrimSpace(os.Getenv("OSS_ENDPOINT")),
			Bucket:         strings.TrimSpace(os.Getenv("OSS_BUCKET")),
			CredentialMode: strings.ToLower(envOr("OSS_CREDENTIAL_MODE", "env")),
		},
		MediaTools: MediaTools{
			FFmpegPath:  envOr("FFMPEG_PATH", "ffmpeg"),
			FFprobePath: envOr("FFPROBE_PATH", "ffprobe"),
			TempDir:     envOr("MEDIA_TEMP_DIR", os.TempDir()+"/mirrorcore-media"),
		},
		Bailian: Bailian{
			Region:      envOr("BAILIAN_REGION", "cn-beijing"),
			WorkspaceID: strings.TrimSpace(os.Getenv("BAILIAN_WORKSPACE_ID")),
			APIKey:      strings.TrimSpace(os.Getenv("BAILIAN_API_KEY")),
		},
		BootstrapEmail:    strings.ToLower(strings.TrimSpace(os.Getenv("BOOTSTRAP_EMAIL"))),
		BootstrapPassword: os.Getenv("BOOTSTRAP_PASSWORD"),
	}
	secure, err := strconv.ParseBool(envOr("COOKIE_SECURE", "true"))
	if err != nil {
		return Config{}, fmt.Errorf("COOKIE_SECURE: %w", err)
	}
	cfg.CookieSecure = secure
	localDemo, err := strconv.ParseBool(envOr("LOCAL_DEMO_MODE", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("LOCAL_DEMO_MODE: %w", err)
	}
	cfg.LocalDemoMode = localDemo
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS must contain at least one broker")
	}
	if err := validateListenAddr("API_METRICS_ADDR", cfg.APIMetricsAddr); err != nil {
		return Config{}, err
	}
	if err := validateListenAddr("BACKGROUND_METRICS_ADDR", cfg.BackgroundMetricsAddr); err != nil {
		return Config{}, err
	}
	if cfg.ObjectStorage.Bucket != "" && cfg.ObjectStorage.Region == "" {
		return Config{}, errors.New("OSS_REGION is required when OSS_BUCKET is configured")
	}
	if cfg.ObjectStorage.Endpoint != "" && cfg.ObjectStorage.Bucket == "" {
		return Config{}, errors.New("OSS_BUCKET is required when OSS_ENDPOINT is configured")
	}
	if cfg.ObjectStorage.CredentialMode != "env" && cfg.ObjectStorage.CredentialMode != "ecs" {
		return Config{}, errors.New("OSS_CREDENTIAL_MODE must be env or ecs")
	}
	if cfg.Bailian.Region != "cn-beijing" {
		return Config{}, errors.New("BAILIAN_REGION must be cn-beijing for the selected Fun-ASR provider")
	}
	if (cfg.Bailian.WorkspaceID == "") != (cfg.Bailian.APIKey == "") {
		return Config{}, errors.New("BAILIAN_WORKSPACE_ID and BAILIAN_API_KEY must be configured together")
	}
	if cfg.Bailian.WorkspaceID != "" {
		if len(cfg.Bailian.WorkspaceID) > 128 {
			return Config{}, errors.New("BAILIAN_WORKSPACE_ID is too long")
		}
		for _, r := range cfg.Bailian.WorkspaceID {
			if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '-' {
				return Config{}, errors.New("BAILIAN_WORKSPACE_ID contains invalid characters")
			}
		}
		if cfg.Bailian.EmbeddingRPM, err = positiveIntEnv("BAILIAN_EMBEDDING_RPM", 1_000_000); err != nil {
			return Config{}, err
		}
		if cfg.Bailian.EmbeddingTPM, err = positiveInt64Env("BAILIAN_EMBEDDING_TPM", 1_000_000_000); err != nil {
			return Config{}, err
		}
		if cfg.Bailian.QwenRPM, err = positiveIntEnv("BAILIAN_QWEN_RPM", 1_000_000); err != nil {
			return Config{}, err
		}
		if cfg.Bailian.QwenTPM, err = positiveInt64Env("BAILIAN_QWEN_TPM", 1_000_000_000); err != nil {
			return Config{}, err
		}
	}
	if cfg.LocalDemoMode {
		httpHost, _, httpErr := net.SplitHostPort(cfg.HTTPAddr)
		if httpErr != nil || !isLoopbackHost(httpHost) {
			return Config{}, errors.New("LOCAL_DEMO_MODE requires HTTP_ADDR to bind to localhost or a loopback IP")
		}
		apiMetricsHost, _, apiMetricsErr := net.SplitHostPort(cfg.APIMetricsAddr)
		backgroundMetricsHost, _, backgroundMetricsErr := net.SplitHostPort(cfg.BackgroundMetricsAddr)
		if apiMetricsErr != nil || !isLoopbackHost(apiMetricsHost) ||
			backgroundMetricsErr != nil || !isLoopbackHost(backgroundMetricsHost) {
			return Config{}, errors.New("LOCAL_DEMO_MODE requires metrics listeners to bind to localhost or a loopback IP")
		}
		parsed, parseErr := url.Parse(cfg.DatabaseURL)
		if parseErr != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" {
			return Config{}, errors.New("LOCAL_DEMO_MODE requires a PostgreSQL URL on localhost or a loopback IP")
		}
		if !isLoopbackHost(parsed.Hostname()) {
			return Config{}, errors.New("LOCAL_DEMO_MODE refuses non-loopback DATABASE_URL hosts")
		}
		for _, broker := range cfg.KafkaBrokers {
			brokerHost, _, brokerErr := net.SplitHostPort(broker)
			if brokerErr != nil || !isLoopbackHost(brokerHost) {
				return Config{}, errors.New("LOCAL_DEMO_MODE requires every KAFKA_BROKERS host to be loopback")
			}
		}
		if cfg.ObjectStorage.Enabled() || cfg.Bailian.WorkspaceID != "" || cfg.Bailian.APIKey != "" {
			return Config{}, errors.New("LOCAL_DEMO_MODE cannot be combined with OSS or Bailian credentials")
		}
	}
	if _, err := mail.ParseAddress(cfg.BootstrapEmail); err != nil {
		return Config{}, fmt.Errorf("BOOTSTRAP_EMAIL must be a valid email address")
	}
	if len(cfg.BootstrapPassword) < 12 || len(cfg.BootstrapPassword) > 72 {
		return Config{}, errors.New("BOOTSTRAP_PASSWORD must be between 12 and 72 bytes")
	}
	return cfg, nil
}

func validateListenAddr(name, value string) error {
	_, rawPort, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be a host:port address: %w", name, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("%s must use a TCP port from 1 to 65535", name)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func positiveIntEnv(name string, maximum int64) (int, error) {
	value, err := positiveInt64Env(name, maximum)
	if err != nil {
		return 0, err
	}
	return int(value), nil
}

func positiveInt64Env(name string, maximum int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, fmt.Errorf("%s is required when Bailian is enabled; configure the account's per-model quota", name)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || value > maximum {
		return 0, fmt.Errorf("%s must be an integer between 1 and %d", name, maximum)
	}
	return value, nil
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			items = append(items, part)
		}
	}
	return items
}
