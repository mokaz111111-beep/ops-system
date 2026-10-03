// Command otlp-loader 是 PLAN B4 的落库进程。
//
// Kafka consumer → 只认消息头身份 → OTLP 展开 → IF-3 纯函数切批 → Stream Load。
// 配置只读本地文件；消费路径不查控制面。
//
//	otlp-loader -config loader.json
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mokaz/ops-system/internal/loader/config"
	"github.com/mokaz/ops-system/internal/loader/flatten"
	"github.com/mokaz/ops-system/internal/loader/kafkasrc"
	"github.com/mokaz/ops-system/internal/loader/pipeline"
	"github.com/mokaz/ops-system/internal/loader/selfmon"
	"github.com/mokaz/ops-system/internal/loader/streamload"
	"github.com/mokaz/ops-system/pkg/latency"
)

func main() {
	configPath := flag.String("config", "", "配置文件路径")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "必须指定 -config")
		os.Exit(2)
	}
	if err := run(log, *configPath); err != nil {
		log.Error("loader 退出", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	metrics := selfmon.New()
	src, err := newSource(cfg, log)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	load, err := newLoadClient(cfg, log)
	if err != nil {
		return err
	}
	defer func() { _ = load.Close() }()

	pipe := pipeline.New(src, load, pipeline.Config{
		Params:     cfg.Params(),
		IdleSubmit: cfg.Batch.IdleSubmit.Get(2 * time.Second),
		Flatten:    flatten.DefaultOptions(),
		TableLogs:  cfg.Doris.TableLogs,
		TableSpans: cfg.Doris.TableSpans,
	}, metrics, log)

	ready.Store(true)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	admin := newAdminServer(cfg, metrics)
	errc := make(chan error, 2)
	go func() {
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("admin 监听失败: %w", err)
		}
	}()
	go func() { errc <- pipe.Run(ctx) }()

	logStartup(log, cfg)

	select {
	case err := <-errc:
		if errors.Is(err, context.Canceled) {
			break
		}
		return err
	case <-ctx.Done():
		log.Info("收到停机信号")
	}

	grace := cfg.ShutdownGrace.Get(15 * time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	_ = admin.Shutdown(shutdownCtx)
	return nil
}

func newSource(cfg config.Config, log *slog.Logger) (kafkasrc.Source, error) {
	switch cfg.Kafka.Mode {
	case "memory":
		log.Warn("Kafka 为 memory 模式，不会消费任何真实消息，仅限本地冒烟")
		return kafkasrc.NewFake(), nil
	case "kafka":
		var tlsCfg *tls.Config
		if cfg.Kafka.TLSEnabled {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return kafkasrc.NewKafka(kafkasrc.KafkaConfig{
			Brokers:  cfg.Kafka.Brokers,
			GroupID:  cfg.Kafka.GroupID,
			ClientID: cfg.Kafka.ClientID,
			Topics:   cfg.Kafka.Topics,
			TLS:      tlsCfg,
		})
	default:
		return nil, fmt.Errorf("未知的 kafka.mode %q", cfg.Kafka.Mode)
	}
}

func newLoadClient(cfg config.Config, log *slog.Logger) (streamload.Client, error) {
	switch cfg.Doris.Mode {
	case "fake":
		log.Warn("Doris 为 fake 模式，Stream Load 不落库，仅限本地冒烟")
		return streamload.NewFake(), nil
	case "http":
		return streamload.NewHTTP(streamload.HTTPConfig{
			FE:         cfg.Doris.FE,
			Database:   cfg.Doris.Database,
			User:       cfg.Doris.User,
			Password:   cfg.Doris.Password,
			TableLogs:  cfg.Doris.TableLogs,
			TableSpans: cfg.Doris.TableSpans,
		})
	default:
		return nil, fmt.Errorf("未知的 doris.mode %q", cfg.Doris.Mode)
	}
}

func newAdminServer(cfg config.Config, metrics *selfmon.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	})
	return &http.Server{
		Addr:              cfg.AdminListen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

var ready atomic.Bool

func logStartup(log *slog.Logger, cfg config.Config) {
	stages := make([]string, 0, 2)
	for _, s := range latency.StagesFor(latency.ComponentLoader) {
		stages = append(stages, string(s.Stage))
	}
	log.Info("otlp-loader 启动",
		"admin_listen", cfg.AdminListen,
		"kafka_mode", cfg.Kafka.Mode,
		"kafka_brokers", cfg.Kafka.Brokers,
		"topics", cfg.Kafka.Topics,
		"doris_mode", cfg.Doris.Mode,
		"stride", cfg.Batch.Stride,
		"max_encoded_bytes", cfg.Batch.MaxEncodedBytes,
		"latency_stages", stages,
	)
}
