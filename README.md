# Microservices with Consul — Banking System

FastAPI microservices implementing a banking transaction system using **Consul** as Service Register, Service Discovery, and Config Server, with a **Hazelcast** distributed map and message queue.

## Services

| Service | Internal port | Host port(s) |
|---|---|---|
| `facade-service` | 8000 | 8000 |
| `logging-service` ×3 | 8001 | 8011, 8012, 8013 |
| `counter-service` | 8002 | 8002 |
| `consul` | 8500 | 8500 |
| `hazelcast-1/2/3` | 5701 | 5701, 5702, 5703 |

## Architecture

```
Client
  │
  ▼ HTTP POST/GET
Facade-service ◄──► Consul (service discovery + KV config)
  │  │                      ▲           ▲           ▲
  │  │ HTTP POST (log)       │           │           │
  │  └─────────────────► logging-svc ×3 │      counter-svc
  │                      (registers)    │      (registers)
  │ Hazelcast Queue (MQ)                │
  └─────────────────────────────────► counter-service
                                      (consumes from queue)
                HZ cluster (3 nodes) ◄── shared distributed map
```

### Consul roles

| Role | What it does |
|---|---|
| **Service Register** | All services (`facade`, `logging` ×3, `counter`) register themselves on startup with an HTTP health check |
| **Service Discovery** | `facade-service` calls `consul.health.service(name, passing=True)` to get a live list of healthy instances and picks one at random |
| **Config KV** | Hazelcast and MQ settings stored as key/value pairs; services read these at startup instead of using hardcoded env vars |

### Consul KV store

| Key | Value | Read by |
|---|---|---|
| `config/hazelcast/hosts` | `hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701` | `logging-service` |
| `config/hazelcast/cluster_name` | `dev` | `logging-service` |
| `config/hazelcast/map_name` | `logging-messages` | `logging-service` |
| `config/mq/hosts` | `hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701` | `facade-service`, `counter-service` |
| `config/mq/cluster_name` | `dev` | `facade-service`, `counter-service` |
| `config/mq/queue_name` | `counter-transactions` | `facade-service`, `counter-service` |

The `consul-init` container seeds all keys on first boot and then exits.

### Facade Service (port 8000)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/transactions` | Log transaction + enqueue to counter-service MQ |
| `GET` | `/user/{user_id}` | Balance + transaction history |
| `GET` | `/accounts` | All account balances |
| `GET` | `/stats` | Accumulated call times to downstream services |
| `POST` | `/stats/reset` | Reset timing accumulators |
| `GET` | `/health` | Health check (used by Consul) |

On `POST /transactions` the facade:
1. Queries Consul for healthy `logging-service` instances, picks one at random, calls `POST /log` (synchronous).
2. Puts the transaction JSON into the Hazelcast `counter-transactions` queue (fire-and-forget).

### Logging Service (ports 8011–8013, 3 instances)

Stores transactions in a **Hazelcast distributed map** (`logging-messages`) shared across all instances.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/log` | Store `{transaction_id, user_id, amount}` (deduplication by `transaction_id`) |
| `GET` | `/messages` | All stored transactions |
| `GET` | `/user/{user_id}` | Transactions for one user |
| `GET` | `/health` | Health check (used by Consul) |

### Counter Service (port 8002)

Runs a background thread that **consumes** from the Hazelcast `counter-transactions` queue and updates in-memory balances.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/balance/{user_id}` | Balance for one user |
| `GET` | `/balances` | All account balances |
| `GET` | `/health` | Health check (used by Consul) |

### Hazelcast Cluster (3 nodes, ports 5701–5703)

Provides both the **distributed map** (logging-service) and the **distributed queue** (MQ between facade and counter).

## Running

```bash
docker compose up --build
```

To stop and clean up:

```bash
docker compose down
```

## Verification Output

### 1. All services registered in Consul (healthy)

```
$ curl -s http://localhost:8500/v1/health/state/passing | python3 -m json.tool | grep -E '"ServiceName"|"ServiceID"'
        "ServiceID": "",
        "ServiceName": "",
        "ServiceID": "counter-service-counter-service",
        "ServiceName": "counter-service",
        "ServiceID": "facade-service-facade-service",
        "ServiceName": "facade-service",
        "ServiceID": "logging-service-logging-service-1",
        "ServiceName": "logging-service",
        "ServiceID": "logging-service-logging-service-2",
        "ServiceName": "logging-service",
        "ServiceID": "logging-service-logging-service-3",
        "ServiceName": "logging-service",
```

All 5 service instances are passing health checks. The empty `ServiceID`/`ServiceName` entry is the Consul node itself.

### 2. Consul KV store (config seeded by consul-init)

Values are base64-encoded by the Consul API (decoded values shown in comments):

```
$ curl -s 'http://localhost:8500/v1/kv/config?recurse=true' | python3 -m json.tool
[
    { "Key": "config/hazelcast/cluster_name", "Value": "ZGV2"              },  // dev
    { "Key": "config/hazelcast/hosts",        "Value": "aGF6ZWxjYXN0..."  },  // hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701
    { "Key": "config/hazelcast/map_name",     "Value": "bG9nZ2luZy1..."   },  // logging-messages
    { "Key": "config/mq/cluster_name",        "Value": "ZGV2"              },  // dev
    { "Key": "config/mq/hosts",               "Value": "aGF6ZWxjYXN0..."  },  // hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701
    { "Key": "config/mq/queue_name",          "Value": "Y291bnRlci0..."   }   // counter-transactions
]
```

### 3. POST transactions

```
$ curl -s -X POST http://localhost:8000/transactions \
    -H 'Content-Type: application/json' \
    -d '{"user_id": "alice", "amount": 100.0}' | python3 -m json.tool
{
    "transaction_id": "d6c46649-c623-4427-a09e-02a3b1088b4f",
    "status": "queued",
    "logged": {
        "status": "stored",
        "transaction_id": "d6c46649-c623-4427-a09e-02a3b1088b4f"
    }
}

$ curl -s -X POST http://localhost:8000/transactions \
    -H 'Content-Type: application/json' \
    -d '{"user_id": "bob", "amount": 50.0}' | python3 -m json.tool
{
    "transaction_id": "e956dc76-60ef-4fb5-ac6f-38678023ed1c",
    "status": "queued",
    "logged": {
        "status": "stored",
        "transaction_id": "e956dc76-60ef-4fb5-ac6f-38678023ed1c"
    }
}
```

### 4. GET user balance and all accounts

```
$ curl -s http://localhost:8000/user/alice | python3 -m json.tool
{
    "balance": 100.0,
    "transactions": [
        {
            "transaction_id": "d6c46649-c623-4427-a09e-02a3b1088b4f",
            "user_id": "alice",
            "amount": 100.0
        }
    ]
}

$ curl -s http://localhost:8000/accounts | python3 -m json.tool
{
    "alice": 100.0,
    "bob": 50.0
}
```

### 5. Fault-tolerance demo — instance failure redirected automatically

```
$ sudo docker stop logging-service-2
logging-service-2

$ sleep 12   # Consul marks it critical after health check interval

$ curl -s -X POST http://localhost:8000/transactions \
    -H 'Content-Type: application/json' \
    -d '{"user_id": "alice", "amount": 10.0}' | python3 -m json.tool
{
    "transaction_id": "f9be60da-16d9-4259-b541-ad26e90df4c9",
    "status": "queued",
    "logged": {
        "status": "stored",
        "transaction_id": "f9be60da-16d9-4259-b541-ad26e90df4c9"
    }
}

$ sudo docker start logging-service-2
logging-service-2
```

Request succeeded with `logging-service-2` down — Consul only returned healthy instances (`logging-service-1` and `logging-service-3`), so the facade routed to one of those automatically.

### 6. Performance tests

**Scenario 1 — 10 accounts (10 different user_ids), 10 POST /transactions:**

```
$ curl -s -X POST http://localhost:8000/stats/reset
$ for i in $(seq 1 10); do
    curl -s -X POST http://localhost:8000/transactions \
      -H 'Content-Type: application/json' \
      -d "{\"user_id\": \"user$i\", \"amount\": 10.0}" > /dev/null
  done
$ curl -s http://localhost:8000/stats | python3 -m json.tool
{
    "logging_service": {
        "total_time_s": 0.185942,
        "call_count": 10,
        "avg_time_s": 0.018594
    },
    "counter_service": {
        "total_time_s": 0.0,
        "call_count": 0,
        "avg_time_s": 0
    }
}
```

**Scenario 2 — 1 account (same user_id), 10 POST /transactions:**

```
$ curl -s -X POST http://localhost:8000/stats/reset
$ for i in $(seq 1 10); do
    curl -s -X POST http://localhost:8000/transactions \
      -H 'Content-Type: application/json' \
      -d '{"user_id": "alice", "amount": 10.0}' > /dev/null
  done
$ curl -s http://localhost:8000/stats | python3 -m json.tool
{
    "logging_service": {
        "total_time_s": 0.174772,
        "call_count": 10,
        "avg_time_s": 0.017477
    },
    "counter_service": {
        "total_time_s": 0.0,
        "call_count": 0,
        "avg_time_s": 0
    }
}
```

**Performance summary (logging-service contribution = total_time_s / 10 requests):**

| Test scenario | Total time | logging-service contribution | counter-service contribution |
|---|---|---|---|
| 10 accounts | 0.185942 s | 0.018594 s/req (avg) | via MQ (async, not timed) |
| 1 account | 0.174772 s | 0.017477 s/req (avg) | via MQ (async, not timed) |

> Note: `counter-service` receives transactions via the Hazelcast MQ asynchronously (fire-and-forget), so it does not contribute to the POST response time. Its contribution is measured separately via `GET /user/{id}` calls.

## Consul UI

Open **http://localhost:8500** in a browser to see:
- All registered services with instance counts and health status
- Key/Value store with the config entries
- Real-time health check results

## Project Structure

```
micro_services_architevture/
├── docker-compose.yml
├── hazelcast/
│   └── hazelcast.xml          # TCP/IP cluster discovery config
├── facade-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                # Consul discovery + MQ config from KV
├── logging-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                # Consul registration + HZ config from KV
├── counter-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                # Consul registration + MQ config from KV
└── perf/
    └── bench.py
```

## Screenshots

![res1](assests/basic_example/image1.png)
![res2](assests/basic_example/image2.png)
![res3](assests/basic_example/image3.png)
![res4](assests/basic_example/image4.png)
