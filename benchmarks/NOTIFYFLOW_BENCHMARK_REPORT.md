# NotifyFlow API Benchmark Report

## Test configuration
- Requests per run: 2,000
- Concurrency: 20
- Warm-up requests: 20 (excluded from measured results)
- API database connection pool: 15 connections
- Maximum overflow connections: 0
- Pool timeout: 30 seconds
- Environment: local development setup

## Results

| Run | Throughput (req/s) | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|
| 1 | 325.64 | 61.03 | 60.52 | 78.10 | 88.65 |
| 2 | 319.07 | 62.31 | 61.27 | 80.72 | 109.00 |
| 3 | 324.49 | 61.33 | 61.26 | 76.66 | 84.72 |

## Three-run median
- Throughput: **324.49 requests/second**
- Average latency: **61.33 ms**
- p50 latency: **61.26 ms**
- p95 latency: **78.10 ms**
- p99 latency: **88.65 ms**
- HTTP acceptance success: **100% (2,000/2,000 per run)**

## Comparison with earlier baseline
- Earlier baseline: 2,000 requests, concurrency 20, median of five runs.
- Baseline throughput: 198.95 requests/second.
- Baseline p95 latency: 147.603 ms.
- Observed throughput difference: **+63.10%**.
- Observed p95 latency reduction: **47.09%**.

These are observed differences between benchmark sessions, not proof
that the connection-pool change alone caused the improvement.

## Important limitations
- HTTP 202 means the API accepted the notification; it does not prove SMS delivery.
- Tests ran against a local development environment, not a production deployment.
- Warm-up requests create notification records but are excluded from measured CSV results.
- Hardware specifications and resource utilization were not recorded for these runs.
- Throughput values are taken from the benchmark console output.
- Latency percentiles are recalculated from each run's CSV.
