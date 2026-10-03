# 可选本机验证：拉起单节点 Kafka、按 IF-2 建 topic、跑默认 skip 的 live produce 测试。
# 不替代 go test ./...。没有 Docker 时直接失败并说明原因。

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "未找到 docker。B2 的本机验证需要 Docker Desktop；无 Docker 时请只用 go test（Fake）或对接已有集群。"
}

$compose = Join-Path $RepoRoot "deploy/kafka/docker-compose.yml"
docker compose -f $compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Error "docker compose up 失败"
}

$deadline = (Get-Date).AddMinutes(2)
do {
    docker compose -f $compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    if ((Get-Date) -gt $deadline) {
        Write-Error "Kafka 在 2 分钟内没有就绪"
    }
    Start-Sleep -Seconds 3
} while ($true)

go run ./cmd/ensure-kafka-topics -brokers 127.0.0.1:9092 -profile dev
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

go run ./cmd/ensure-kafka-topics -brokers 127.0.0.1:9092 -profile dev -check-only
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$env:OPS_KAFKA_IT = "1"
$env:OPS_KAFKA_BROKERS = "127.0.0.1:9092"
go test ./internal/gateway/kafkasink -run TestProduceLiveKafka -count=1
exit $LASTEXITCODE
