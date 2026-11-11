package config

import "testing"

func TestLoadRequiresDatabaseURL(t *testing.T) {
	setBaseConfig(t)
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected DATABASE_URL validation error")
	}
}

func TestLoadAcceptsDevelopmentConfiguration(t *testing.T) {
	setBaseConfig(t)
	t.Setenv("DATABASE_URL", "postgres://mirrorcore:secret@localhost:5432/mirrorcore?sslmode=disable")
	t.Setenv("COOKIE_SECURE", "false")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CookieSecure {
		t.Fatal("expected insecure cookie for local HTTP development")
	}
}

func TestLoadParsesKafkaBrokers(t *testing.T) {
	setBaseConfig(t)
	t.Setenv("KAFKA_BROKERS", " kafka-a:9092, ,kafka-b:9092 ")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka-a:9092" || cfg.KafkaBrokers[1] != "kafka-b:9092" {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
}

func setBaseConfig(t *testing.T) {
	t.Helper()
	t.Setenv("HTTP_ADDR", ":8080")
	t.Setenv("DATABASE_URL", "postgres://mirrorcore:secret@localhost:5432/mirrorcore?sslmode=disable")
	t.Setenv("BOOTSTRAP_EMAIL", "demo@example.test")
	t.Setenv("BOOTSTRAP_PASSWORD", "correct-horse-battery-staple")
	t.Setenv("COOKIE_SECURE", "true")
}
