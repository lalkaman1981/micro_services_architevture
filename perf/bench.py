#!/usr/bin/env python3
"""Async benchmark client for the banking microservices facade.

Scenarios
---------
1 (separate)  - 10 clients each POST 10K "+1" transactions to their own account.
                Expected result: 10 accounts each with balance 10_000.
2 (shared)    - 10 clients each POST 10K "+1" transactions to the same account.
                Expected result: 1 account with balance 100_000.

Usage
-----
# Scenario 1
python bench.py --scenario 1 --clients 10 --per-client 10000

# Scenario 2
python bench.py --scenario 2 --clients 10 --per-client 10000

# Custom one-off load (arbitrary payload)
python bench.py --url http://localhost:8000/transactions \\
    --data '{"user_id":"alice","amount":1.0}' --clients 5 --per-client 100
"""
import argparse
import asyncio
import json
import time
from statistics import mean, median

import httpx

FACADE_URL = "http://localhost:8000"


async def worker(worker_id: int, client: httpx.AsyncClient, url: str, n: int, payload: dict, results: list):
    for _ in range(n):
        start = time.perf_counter()
        status = None
        try:
            r = await client.post(url, json=payload)
            status = r.status_code
        except Exception:
            pass
        results.append((time.perf_counter() - start, status))


async def run_scenario(scenario: int, clients: int, per_client: int, base_url: str):
    url = f"{base_url}/transactions"
    total = clients * per_client
    results = []

    print(f"Scenario {scenario}: {clients} clients x {per_client} requests = {total} total")
    if scenario == 1:
        print("Each client posts to its own account (user_1 … user_N)")
    else:
        print("All clients post to the same account (shared_user)")

    start_all = time.perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = []
        for i in range(clients):
            user_id = f"user_{i + 1}" if scenario == 1 else "shared_user"
            payload = {"user_id": user_id, "amount": 1.0}
            tasks.append(asyncio.create_task(worker(i, client, url, per_client, payload, results)))
        await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_all

    _print_results(results, elapsed, total)

    # Fetch and display final balances from facade
    print("\nFetching final balances (/accounts)…")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(f"{base_url}/accounts")
            if r.status_code == 200:
                print(f"Balances: {json.dumps(r.json(), indent=2)}")
        except Exception as e:
            print(f"Could not fetch balances: {e}")

    # Fetch timing stats from facade
    print("\nFetching downstream timing stats (/stats)…")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(f"{base_url}/stats")
            if r.status_code == 200:
                stats = r.json()
                ls = stats["logging_service"]
                cs = stats["counter_service"]
                print(f"logging-service : {ls['call_count']} calls, total={ls['total_time_s']:.3f}s, avg={ls['avg_time_s']*1000:.2f}ms")
                print(f"counter-service : {cs['call_count']} calls, total={cs['total_time_s']:.3f}s, avg={cs['avg_time_s']*1000:.2f}ms")
        except Exception as e:
            print(f"Could not fetch stats: {e}")


async def run_custom(url: str, clients: int, per_client: int, payload: dict, base_url: str):
    total = clients * per_client
    results = []
    print(f"Custom load: {clients} clients x {per_client} requests = {total} total → {url}")

    start_all = time.perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = [
            asyncio.create_task(worker(i, client, url, per_client, payload, results))
            for i in range(clients)
        ]
        await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_all

    _print_results(results, elapsed, total)


def _print_results(results: list, elapsed: float, total: int):
    durations = [r[0] for r in results]
    ok = sum(1 for _, s in results if s and 200 <= s < 300)

    print("\n--- Benchmark results ---")
    print(f"total_requests      : {len(results)}  (expected {total})")
    print(f"successful_responses: {ok}")
    print(f"wall_time_seconds   : {elapsed:.3f}")
    print(f"requests_per_second : {len(results) / elapsed:.1f}")
    if durations:
        print(f"avg_latency_ms      : {mean(durations) * 1000:.2f}")
        print(f"median_latency_ms   : {median(durations) * 1000:.2f}")
        print(f"min_latency_ms      : {min(durations) * 1000:.2f}")
        print(f"max_latency_ms      : {max(durations) * 1000:.2f}")


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark client for the banking microservices facade")
    p.add_argument("--scenario", type=int, choices=[1, 2], help="Run predefined scenario 1 or 2")
    p.add_argument("--url", default=f"{FACADE_URL}/transactions", help="POST URL for custom load")
    p.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    p.add_argument("--per-client", type=int, default=100, help="Requests per client")
    p.add_argument("--data", default='{"user_id":"bench_user","amount":1.0}', help="JSON payload for custom load")
    p.add_argument("--base-url", default=FACADE_URL, help="Base URL for facade-service (used for /accounts and /stats)")
    return p.parse_args()


def main():
    args = parse_args()

    if args.scenario:
        asyncio.run(run_scenario(args.scenario, args.clients, args.per_client, args.base_url))
    else:
        try:
            payload = json.loads(args.data)
        except Exception:
            print("Failed to parse --data as JSON")
            return
        asyncio.run(run_custom(args.url, args.clients, args.per_client, payload, args.base_url))


if __name__ == "__main__":
    main()
