import csv
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent

RUNS = [
    ("api_results_20261120_062008_245453.csv", 6.142, 325.64),
    ("api_results_20261120_062208_639164.csv", 6.268, 319.07),
    ("api_results_20261120_062325_586058.csv", 6.164, 324.49),
]

def percentile(values, p):
    values = sorted(values)
    pos = (len(values) - 1) * p / 100
    low = int(pos)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (pos - low)

results = []

for filename, duration, reported_throughput in RUNS:
    with (ROOT / filename).open(newline="") as f:
        rows = list(csv.DictReader(f))

    successful = [
        r for r in rows
        if r["success"].lower() == "true"
        and r["status_code"] == "202"
    ]
    latencies = [float(r["latency_ms"]) for r in successful]

    if len(rows) != 2000 or len(successful) != 2000:
        raise ValueError(f"Unexpected request count or status in {filename}")

    results.append({
        "file": filename,
        "throughput": reported_throughput,
        "avg": statistics.mean(latencies),
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
    })

def median(key):
    return statistics.median(r[key] for r in results)

throughput_gain = (median("throughput") / 198.95 - 1) * 100
p95_reduction = (1 - median("p95") / 147.603) * 100

lines = [
    "# NotifyFlow API Benchmark Report",
    "",
    "## Test configuration",
    "- Requests per run: 2,000",
    "- Concurrency: 20",
    "- Warm-up requests: 20 (excluded from measured results)",
    "- API database connection pool: 15 connections",
    "- Maximum overflow connections: 0",
    "- Pool timeout: 30 seconds",
    "- Environment: local development setup",
    "",
    "## Results",
    "",
    "| Run | Throughput (req/s) | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |",
    "|---|---:|---:|---:|---:|---:|",
]

for i, r in enumerate(results, 1):
    lines.append(
        f"| {i} | {r['throughput']:.2f} | {r['avg']:.2f} | "
        f"{r['p50']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} |"
    )

lines += [
    "",
    "## Three-run median",
    f"- Throughput: **{median('throughput'):.2f} requests/second**",
    f"- Average latency: **{median('avg'):.2f} ms**",
    f"- p50 latency: **{median('p50'):.2f} ms**",
    f"- p95 latency: **{median('p95'):.2f} ms**",
    f"- p99 latency: **{median('p99'):.2f} ms**",
    "- HTTP acceptance success: **100% (2,000/2,000 per run)**",
    "",
    "## Comparison with earlier baseline",
    "- Earlier baseline: 2,000 requests, concurrency 20, median of five runs.",
    "- Baseline throughput: 198.95 requests/second.",
    "- Baseline p95 latency: 147.603 ms.",
    f"- Observed throughput difference: **+{throughput_gain:.2f}%**.",
    f"- Observed p95 latency reduction: **{p95_reduction:.2f}%**.",
    "",
    "These are observed differences between benchmark sessions, not proof",
    "that the connection-pool change alone caused the improvement.",
    "",
    "## Important limitations",
    "- HTTP 202 means the API accepted the notification; it does not prove SMS delivery.",
    "- Tests ran against a local development environment, not a production deployment.",
    "- Warm-up requests create notification records but are excluded from measured CSV results.",
    "- Hardware specifications and resource utilization were not recorded for these runs.",
    "- Throughput values are taken from the benchmark console output.",
    "- Latency percentiles are recalculated from each run's CSV.",
    "",
]

output = ROOT / "NOTIFYFLOW_BENCHMARK_REPORT.md"
output.write_text("\n".join(lines), encoding="utf-8")
print(f"Created: {output}")
print(f"Median throughput: {median('throughput'):.2f} req/s")
print(f"Median p95: {median('p95'):.2f} ms")
print(f"Observed throughput difference: +{throughput_gain:.2f}%")
print(f"Observed p95 reduction: {p95_reduction:.2f}%")
