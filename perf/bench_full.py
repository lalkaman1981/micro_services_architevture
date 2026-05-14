"""POST + GET combined benchmark.

Runs the scenario's POSTs, lets the MQ drain, then runs an equal number of
GETs (so counter-service contribution is measured even when POST uses MQ).
"""
import argparse
import asyncio
import time
from statistics import mean, median

import httpx

FACADE = "http://localhost:8000"


async def worker_post(client, url, n, payload, results):
    for _ in range(n):
        t = time.perf_counter()
        try:
            r = await client.post(url, json=payload)
            results.append((time.perf_counter() - t, r.status_code))
        except Exception:
            results.append((time.perf_counter() - t, None))


async def worker_get(client, url_fn, n, results):
    for _ in range(n):
        t = time.perf_counter()
        try:
            r = await client.get(url_fn())
            results.append((time.perf_counter() - t, r.status_code))
        except Exception:
            results.append((time.perf_counter() - t, None))


def _print(results, elapsed, label):
    ok = sum(1 for _, s in results if s and 200 <= s < 300)
    durs = [r[0] for r in results]
    print(f"  {label}:")
    print(f"    total           : {len(results)}  (ok={ok})")
    print(f"    wall_time_s     : {elapsed:.3f}")
    print(f"    rps             : {len(results) / elapsed:.1f}")
    if durs:
        print(f"    avg_latency_ms  : {mean(durs)*1000:.2f}")
        print(f"    median_lat_ms   : {median(durs)*1000:.2f}")
        print(f"    min/max ms      : {min(durs)*1000:.2f} / {max(durs)*1000:.2f}")


async def main(scenario: int, clients: int, per_client: int):
    total = clients * per_client
    label = "10 separate accounts" if scenario == 1 else "1 shared account"
    print(f"=== Scenario {scenario} ({label}) — {clients} clients x {per_client} = {total} requests ===\n")

    # reset stats
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(f"{FACADE}/stats/reset")

    # -- POST phase --
    post_results = []
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = []
        for i in range(clients):
            user = f"user_{i+1}" if scenario == 1 else "shared_user"
            payload = {"user_id": user, "amount": 1.0}
            tasks.append(asyncio.create_task(
                worker_post(client, f"{FACADE}/transactions", per_client, payload, post_results)))
        await asyncio.gather(*tasks)
    post_elapsed = time.perf_counter() - t0
    _print(post_results, post_elapsed, "POST /transactions")

    # Let the MQ drain so counter is up-to-date for GETs
    await asyncio.sleep(3)

    # -- GET phase --
    get_results = []
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = []
        for i in range(clients):
            user = f"user_{i+1}" if scenario == 1 else "shared_user"
            url_fn = (lambda u=user: f"{FACADE}/user/{u}")
            tasks.append(asyncio.create_task(
                worker_get(client, url_fn, per_client, get_results)))
        await asyncio.gather(*tasks)
    get_elapsed = time.perf_counter() - t0
    _print(get_results, get_elapsed, "GET /user/{id}")

    # totals
    print(f"\n  TOTAL wall time (POST+GET): {post_elapsed + get_elapsed:.3f}s")

    # downstream contributions
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{FACADE}/stats")
    stats = r.json()
    ls = stats["logging_service"]
    cs = stats["counter_service"]
    print("\n  Downstream contributions (from facade /stats):")
    print(f"    logging-service : {ls['call_count']:>4} calls, total={ls['total_time_s']:.3f}s, avg={ls['avg_time_s']*1000:.2f}ms")
    print(f"    counter-service : {cs['call_count']:>4} calls, total={cs['total_time_s']:.3f}s, avg={cs['avg_time_s']*1000:.2f}ms")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", type=int, choices=[1, 2], required=True)
    p.add_argument("--clients", type=int, default=10)
    p.add_argument("--per-client", type=int, default=100)
    args = p.parse_args()
    asyncio.run(main(args.scenario, args.clients, args.per_client))
