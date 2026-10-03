// Command ingest-gateway 是摄入网关（PLAN B1）。
//
// 它把 OTLP/HTTP 请求鉴权、限流、归一后投进 Kafka，并按 SD-000 §8.1 对它负责的那三段
// 延迟分别打点（PLAN B6）。两个监听端口：数据面与管理面分开，理由见 config.Config。
//
// 用法：
//
//	ingest-gateway -config gateway.json
//	ingest-gateway -hash-token tok-xxxx    # 生成快照文件需要的 token_sha256
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/config"
	"github.com/mokaz/ops-system/internal/gateway/ingest"
	"github.com/mokaz/ops-system/internal/gateway/kafkasink"
	"github.com/mokaz/ops-system/internal/gateway/otlphttp"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/internal/gateway/selfmon"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func main() {
	var (
		configPath = flag.String("config", "", "配置文件路径")
		hashToken  = flag.String("hash-token", "", "打印该 Token 的 SHA-256 摘要后退出")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	if *hashToken != "" {
		h, err := identity.HashToken(*hashToken)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println(h)
		return
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "必须指定 -config")
		os.Exit(2)
	}

	if err := run(log, *configPath); err != nil {
		log.Error("网关退出", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	signals, err := cfg.ParsedSignals()
	if err != nil {
		return err
	}

	// 首次快照加载失败即拒绝启动。这不违反 fail-static：fail-static 说的是"运行中控制面
	// 不可用时按最后一次配置继续"，而此刻没有任何"最后一次配置"——带着空快照启动只会
	// 对所有租户返回 E8，还不如让部署流程立刻失败。
	snaps, err := config.LoadSnapshots(cfg.Snapshot)
	if err != nil {
		return err
	}

	ready.Store(true)

	metrics := selfmon.New()
	resolver := authn.NewResolver(snaps.Auth,
		authn.WithClassSTTL(cfg.Snapshot.ClassSTTL.Get(authn.DefaultClassSTTL)))
	limiter := ratelimit.New(snaps.Quota,
		ratelimit.WithIdleTTL(cfg.Limits.IdleTTL.Get(ratelimit.DefaultIdleTTL)))
	metrics.BindSources(resolver, limiter)

	producer, err := newProducer(cfg, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("关闭下游失败", "err", err)
		}
	}()

	router := &kafkasink.Router{Topics: cfg.Topics(), KeyBuckets: cfg.KeyBuckets()}
	pipeline := ingest.New(resolver, limiter, router, producer, metrics,
		ingest.WithMaxBodyBytes(cfg.MaxBodyBytes),
		ingest.WithMaxInflightBytes(cfg.MaxInflightBytes))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go reloadSnapshots(ctx, log, cfg, resolver, limiter)
	go sweepBuckets(ctx, cfg, limiter)

	dataSrv, err := newDataServer(cfg, pipeline, signals)
	if err != nil {
		return err
	}
	adminSrv := newAdminServer(cfg, metrics)

	errc := make(chan error, 2)
	go func() { errc <- serve(dataSrv, cfg.TLS, "data") }()
	go func() { errc <- serve(adminSrv, config.TLSConfig{}, "admin") }()

	logStartup(log, cfg, signals)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Info("收到停机信号，开始优雅停机")
	}

	grace := cfg.ShutdownGrace.Get(15 * time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	// 先停数据面再停管理面：管理面要活到最后，否则停机期间的指标正好缺失，
	// 而滚动发布时的毛刺恰恰发生在这段窗口。
	err = dataSrv.Shutdown(shutdownCtx)
	_ = adminSrv.Shutdown(shutdownCtx)
	return err
}

func newProducer(cfg config.Config, log *slog.Logger) (kafkasink.Producer, error) {
	switch cfg.Kafka.Mode {
	case "memory":
		log.Warn("Kafka 为 memory 模式，数据不落任何持久存储，仅限开发与压测基线使用")
		return kafkasink.NewFake(), nil
	case "kafka":
		var tlsCfg *tls.Config
		if cfg.Kafka.TLSEnabled {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return kafkasink.NewKafka(kafkasink.KafkaConfig{
			Brokers:            cfg.Kafka.Brokers,
			ClientID:           cfg.Kafka.ClientID,
			MaxBufferedRecords: cfg.Kafka.MaxBufferedRecords,
			ProduceTimeout:     time.Duration(cfg.Kafka.ProduceTimeout),
			TLS:                tlsCfg,
		})
	default:
		return nil, fmt.Errorf("未知的 kafka.mode %q", cfg.Kafka.Mode)
	}
}

func newDataServer(cfg config.Config, p *ingest.Pipeline, signals []telemetry.Signal) (*http.Server, error) {
	mux := otlphttp.NewMux(p, signals)
	srv := &http.Server{
		Addr:    cfg.DataListen,
		Handler: mux,
		// 读超时必须大于一个大批次在慢链路上的传输时间，否则会把客户端的慢网络表现为
		// 摄入失败；但也不能没有上限，否则慢连接会占住在途字节额度（E8）。
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := buildTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		srv.TLSConfig = tlsCfg
	}
	return srv, nil
}

func buildTLS(c config.TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("加载证书失败: %w", err)
	}
	out := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if c.ClientCAFile != "" {
		pem, err := os.ReadFile(c.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("读取客户端 CA 失败: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("客户端 CA 文件中没有可用证书")
		}
		out.ClientCAs = pool
		out.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return out, nil
}

func newAdminServer(cfg config.Config, metrics *selfmon.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	// healthz 回答"进程还活着吗"，readyz 回答"能接数据吗"。合成一个端点会让 Kafka 抖动
	// 触发 liveness 重启，而重启只会让在途数据一起丢掉。
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
		_, _ = w.Write([]byte("快照未就绪\n"))
	})

	return &http.Server{
		Addr:              cfg.AdminListen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// ready 表示网关是否已具备服务能力。首次快照在 run 里加载成功后才会建管理服务，
// 因此它在建成时即为 true；保留这个变量是为了 M2 接入控制面 watch 后能把"首次同步完成"
// 与"进程启动"分开。
var ready atomic.Bool

func serve(srv *http.Server, tlsCfg config.TLSConfig, name string) error {
	var err error
	if tlsCfg.Enabled {
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("%s 监听失败: %w", name, err)
}

// reloadSnapshots 周期性重载本地快照。
//
// 重载失败时保留现有快照并记日志——这是 fail-static 的字面实现（DD-006 §3.6）：
// 一次写坏的配置文件或一次磁盘抖动不得让网关拒收数据。
func reloadSnapshots(ctx context.Context, log *slog.Logger, cfg config.Config,
	resolver *authn.Resolver, limiter *ratelimit.Limiter) {

	interval := cfg.Snapshot.ReloadInterval.Get(15 * time.Second)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snaps, err := config.LoadSnapshots(cfg.Snapshot)
			if err != nil {
				log.Error("快照重载失败，继续使用上一份配置", "err", err)
				continue
			}
			resolver.Replace(snaps.Auth)
			limiter.Replace(snaps.Quota)
		}
	}
}

func sweepBuckets(ctx context.Context, cfg config.Config, limiter *ratelimit.Limiter) {
	t := time.NewTicker(cfg.Limits.SweepInterval.Get(time.Minute))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			limiter.Sweep()
		}
	}
}

func logStartup(log *slog.Logger, cfg config.Config, signals []telemetry.Signal) {
	names := make([]string, 0, len(signals))
	for _, s := range signals {
		names = append(names, s.String())
	}
	stages := make([]string, 0, 3)
	for _, s := range latency.StagesFor(latency.ComponentGateway) {
		stages = append(stages, string(s.Stage))
	}
	log.Info("摄入网关启动",
		"data_listen", cfg.DataListen,
		"admin_listen", cfg.AdminListen,
		"tls", cfg.TLS.Enabled,
		"signals", names,
		"kafka_mode", cfg.Kafka.Mode,
		// 把本进程负责的分段列进启动日志：PLAN §2.4 的验收要确认五段都有归属，
		// 从日志里能直接看到网关认领了哪三段，剩下两段该去 loader 找。
		"latency_stages", stages,
	)
}
