
import argparse
import asyncio
import csv
import statistics
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx


def parse_args():
    parser = argparse.ArgumentParser(
        description="NotifyFlow asynchronous API benchmark"
    )

    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--warmup-requests", type=int, default=20)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output-dir", default="benchmarks")

    args = parser.parse_args()

    if args.requests < 1:
        parser.error("--requests must be at least 1")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.warmup_requests < 0:
        parser.error("--warmup-requests cannot be negative")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0")

    return args


def percentile(values, percent):
    ordered = sorted(values)

    if not ordered:
        return None

    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * percent / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower

    return (
        ordered[lower] * (1 - fraction)
        + ordered[upper] * fraction
    )


async def send_request(client, semaphore, request_number, base_url):
    payload = {
        "to": "+919876543210",
        "message": f"NotifyFlow benchmark {uuid.uuid4().hex}",
        "priority": "normal",
    }

    async with semaphore:
        started = time.perf_counter()

        try:
            response = await client.post(
                f"{base_url.rstrip('/')}/notifications",
                json=payload,
            )

            latency_ms = (time.perf_counter() - started) * 1000

            return {
                "request": request_number,
                "status_code": response.status_code,
                "latency_ms": round(latency_ms, 3),
                "success": response.status_code == 202,
                "error": "",
            }

        except Exception as exc:
            latency_ms = (time.perf_counter() - started) * 1000

            return {
                "request": request_number,
                "status_code": 0,
                "latency_ms": round(latency_ms, 3),
                "success": False,
                "error": str(exc),
            }


async def run_requests(client, semaphore, count, base_url):
    return await asyncio.gather(*[
        send_request(client, semaphore, i + 1, base_url)
        for i in range(count)
    ])


async def main():
    args = parse_args()
    semaphore = asyncio.Semaphore(args.concurrency)

    limits = httpx.Limits(
        max_connections=args.concurrency,
        max_keepalive_connections=args.concurrency,
    )

    timeout = httpx.Timeout(args.timeout)

    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
    ) as client:

        print("Starting NotifyFlow API benchmark...")
        print(f"Requests: {args.requests}")
        print(f"Concurrency: {args.concurrency}")
        print(f"Warm-up requests: {args.warmup_requests}")
        print(f"Base URL: {args.base_url}")
        print(f"Timeout: {args.timeout}s")

        if args.warmup_requests:
            print("\nRunning warm-up requests...")

            warmup_results = await run_requests(
                client,
                semaphore,
                args.warmup_requests,
                args.base_url,
            )

            warmup_success = sum(
                result["success"] for result in warmup_results
            )

            print(
                f"Warm-up completed: {warmup_success}/"
                f"{args.warmup_requests} successful"
            )

            if warmup_success != args.warmup_requests:
                raise RuntimeError(
                    "Warm-up failed. Check API availability before benchmarking."
                )

        print("\nStarting measured benchmark...")

        benchmark_started = time.perf_counter()

        results = await run_requests(
            client,
            semaphore,
            args.requests,
            args.base_url,
        )

        total_time = time.perf_counter() - benchmark_started

    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    all_latencies = [r["latency_ms"] for r in results]
    successful_latencies = [r["latency_ms"] for r in successful]

    print("\n========== BENCHMARK RESULTS ==========")
    print(f"Measured requests: {args.requests}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Total duration: {total_time:.3f} seconds")

    if successful_latencies:
        print("\nSuccessful request latency:")
        print(f"Average: {statistics.mean(successful_latencies):.3f} ms")
        print(f"p50: {percentile(successful_latencies, 50):.3f} ms")
        print(f"p95: {percentile(successful_latencies, 95):.3f} ms")
        print(f"p99: {percentile(successful_latencies, 99):.3f} ms")

    throughput = len(successful) / total_time if total_time else 0
    success_rate = len(successful) / args.requests * 100

    print(f"\nThroughput: {throughput:.2f} successful requests/sec")
    print(f"Success rate: {success_rate:.2f}%")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    filename = output_dir / f"api_results_{timestamp}.csv"

    with filename.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "request",
                "status_code",
                "latency_ms",
                "success",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDetailed results saved to: {filename}")


if __name__ == "__main__":
    asyncio.run(main())
