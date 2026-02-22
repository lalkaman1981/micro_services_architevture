#!/usr/bin/env python3
"""Simple async benchmark client for facade-style services.

Features:
- Run N concurrent clients, each sending M requests to the given URL.
- Measure total elapsed time, requests/sec and basic latency statistics.
- Optionally measure average latency of downstream services (logging/counter)
  by sending sample requests directly to them and estimating their contribution.

Usage examples are in the repository README.
"""
import argparse
import asyncio
import json
import time
from statistics import mean, median

import httpx


async def worker(name: int, client: httpx.AsyncClient, url: str, n: int, payload: dict, results: list):
    for i in range(n):
        start = time.perf_counter()
        try:
            r = await client.post(url, json=payload)
            status = r.status_code
        except Exception as e:
            status = None
        dur = time.perf_counter() - start
        results.append((dur, status))


async def measure_service(url: str, samples: int = 50, post: bool = True, payload: dict = None):
    async with httpx.AsyncClient(timeout=10.0) as client:
        times = []
        for _ in range(samples):
            start = time.perf_counter()
            try:
                if post:
                    await client.post(url, json=payload or {})
                else:
                    await client.get(url)
            except Exception:
                # on error, count as a sample but ignore
                times.append(None)
                continue
            times.append(time.perf_counter() - start)
        clean = [t for t in times if t is not None]
        return {
            "samples": len(clean),
            "avg": mean(clean) if clean else None,
            "median": median(clean) if clean else None,
        }


async def run_benchmark(url: str, clients: int, per_client: int, payload: dict, sample_downstream: bool, logging_url: str, counter_url: str):
    total_requests = clients * per_client
    results = []
    start_all = time.perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = [asyncio.create_task(worker(i, client, url, per_client, payload, results)) for i in range(clients)]
        await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_all

    durations = [r[0] for r in results]
    statuses = [r[1] for r in results]
    ok = sum(1 for s in statuses if s and 200 <= s < 300)

    print("--- Benchmark results ---")
    print(f"total_requests: {len(results)} (requested {total_requests})")
    print(f"successful_responses: {ok}")
    print(f"total_wall_time_seconds: {elapsed:.3f}")
    print(f"requests_per_second: {len(results)/elapsed:.3f}")
    if durations:
        print(f"avg_latency_ms: {mean(durations)*1000:.2f}")
        print(f"median_latency_ms: {median(durations)*1000:.2f}")
        print(f"min_latency_ms: {min(durations)*1000:.2f}")
        print(f"max_latency_ms: {max(durations)*1000:.2f}")

    if sample_downstream:
        print('\nMeasuring downstream services (sampled)')
        logging_info = await measure_service(logging_url, samples=50, post=True, payload={})
        counter_info = await measure_service(counter_url, samples=50, post=True, payload={})
        print(f"logging_service avg (s): {logging_info['avg']}")
        print(f"counter_service avg (s): {counter_info['avg']}")
        # Estimate contribution assuming 1 call per request
        est_logging_total = (logging_info['avg'] or 0) * len(results)
        est_counter_total = (counter_info['avg'] or 0) * len(results)
        print(f"estimated_total_time_spent_on_logging(s): {est_logging_total:.3f}")
        print(f"estimated_total_time_spent_on_counter(s): {est_counter_total:.3f}")


def parse_args():
    p = argparse.ArgumentParser(description="Async benchmark client for facade-service")
    p.add_argument("--url", default="http://localhost:8000/messages", help="Facade POST URL")
    p.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    p.add_argument("--per-client", type=int, default=100, help="Number of requests per client")
    p.add_argument("--data", default='{"msg":"1"}', help="JSON payload to send (default '{\"msg\":\"1\"}')")
    p.add_argument("--sample-downstream", action="store_true", help="Also measure downstream service latencies")
    p.add_argument("--logging-url", default="http://localhost:8001/log", help="Logging service URL for sampling")
    p.add_argument("--counter-url", default="http://localhost:8003/counter", help="Counter service URL for sampling (if present)")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        payload = json.loads(args.data)
    except Exception:
        print("Failed to parse --data as JSON")
        return

    print(f"Benchmarking {args.url} with {args.clients} clients x {args.per_client} requests (total {args.clients*args.per_client})")
    print("This may produce a large load — be careful when running in production.")
    asyncio.run(run_benchmark(args.url, args.clients, args.per_client, payload, args.sample_downstream, args.logging_url, args.counter_url))


if __name__ == "__main__":
    main()
