# 本地 Kafka（PLAN B2 / IF-2）

生产判据是 `logs.raw` 12 分区、`traces.raw` 6 分区、**3 副本 + `min.insync.replicas=2`、保留 12 小时**。

本目录的 compose **只提供单节点 KRaft**，用来在本机把网关 `kafka.mode=kafka` 跑通。单节点放不下 3 副本，所以 `ensure-kafka-topics -profile dev` 会把副本因子和 ISR 降到 1。分区数与保留期不降。不要把 `docker compose ps` 当成 B2 生产就绪。

| 项 | 生产（IF-2 / `-profile prod`） | 本仓库 compose（`-profile dev`） |
|----|-------------------------------|----------------------------------|
| 拓扑 | 3 broker 跨 AZ | 1 broker |
| 副本因子 | 3 | **1（降级）** |
| `min.insync.replicas` | 2 | **1（降级）** |
| `logs.raw` 分区 | 12 | 12 |
| `traces.raw` 分区 | 6 | 6 |
| `retention.ms` | 12h | 12h |
| 生产者 `acks` | all | all（网关写死） |
| 压缩 | lz4 | lz4（网关写死；topic 默认也是 lz4） |
| 自动建 topic | 关 | 关 |

## 启动

在仓库根目录：

```powershell
docker compose -f deploy/kafka/docker-compose.yml up -d
# 等到 healthy，或直接跑一键脚本（含等待、建 topic、可选 live produce）：
powershell -File deploy/kafka/verify.ps1
```

手动建 topic（compose 起来之后）：

```powershell
go run ./cmd/ensure-kafka-topics -brokers 127.0.0.1:9092 -profile dev
go run ./cmd/ensure-kafka-topics -brokers 127.0.0.1:9092 -profile dev -check-only
```

生产集群用 `-profile prod`。对已经按 dev 建好的 topic 再跑 prod 会失败并指出副本因子对不上——本工具拒绝自动 reassign。

## 网关切到真 Kafka

默认 `kafka.mode=memory`，`go test ./...` 不依赖本机 Kafka。要真写入：

```powershell
go run ./cmd/ingest-gateway -config deploy/ingest-gateway/gateway.kafka.json
```

或沿用自己的配置文件，只覆盖环境变量：

```
OPS_KAFKA_MODE=kafka
OPS_KAFKA_BROKERS=127.0.0.1:9092
```

开发 Token 明文是 `tok-dev-local`（快照里只有 SHA-256）。不要把生产 Token 写进仓库。

可选集成测试（默认 skip）：

```powershell
$env:OPS_KAFKA_IT = "1"
go test ./internal/gateway/kafkasink -run TestProduceLiveKafka -count=1
```

没有 Docker / 没有已启动的 Kafka 时不要设这个变量。CI 保持默认即可。
