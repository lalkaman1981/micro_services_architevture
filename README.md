# Microservices Basics — tests and run instructions

This repo contains three small FastAPI services used for the homework:

- `facade-service` (port 8000)
- `logging-service` (port 8001)
- `messages-service` (port 8002)

1) Install deps:

```bash
pip install -r requirements.txt
```

2) Start services (run each in its own terminal):

```bash
# facade
uvicorn micro_basics.facade-service.main:app --host 0.0.0.0 --port 8000
# logging
uvicorn micro_basics.logging-service.main:app --host 0.0.0.0 --port 8001
# messages
uvicorn micro_basics.messages-service.main:app --host 0.0.0.0 --port 8002
```

3) Simple tests:

```bash
pytest -q
```

Commands to capture HTTP interactions:

- POST a message via the facade and save output to a file:

```bash
curl -s -X POST http://localhost:8000/messages -H 'Content-Type: application/json' -d '{"msg":"hello from curl"}'
```

- GET combined messages from the facade and save output:

```bash
curl -s http://localhost:8000/messages
```

![res1](assests/basic_example/image1.png)
![res2](assests/basic_example/image2.png)
![res3](assests/basic_example/image3.png)
![res4](assests/basic_example/image4.png)

Notes:

## Overview

This project implements a basic microservices architecture with three independent HTTP-based services that communicate via REST API. The system demonstrates core microservices concepts including service isolation, inter-service communication, retry mechanisms, and deduplication.

## Architecture

### Services

#### 1. Facade Service (Port 8000)
- **Role**: Gateway/orchestrator service that handles client requests
- **Endpoints**:
  - `POST /messages` - Accept a message from client, generate UUID, forward to logging-service
  - `GET /messages` - Aggregate responses from both logging-service and messages-service
- **Features**:
  - UUID generation for each message
  - Exponential backoff retry mechanism (up to 4 attempts)
  - Detailed logging of retry attempts and successes
  - Error handling for downstream service failures

#### 2. Logging Service (Port 8001)
- **Role**: Persistent message storage with deduplication
- **Endpoints**:
  - `POST /log` - Store message with UUID as key
  - `GET /messages` - Retrieve all stored messages
- **Features**:
  - In-memory hash table storage (Dict)
  - Deduplication: exact-once delivery using UUID as key
  - Duplicate detection and logging
  - Detailed logging with timestamps

#### 3. Messages Service (Port 8002)
- **Role**: Static response service (placeholder for future functionality)
- **Endpoints**:
  - `GET /message` - Return static message
- **Response**: "not implemented yet"

## Key Features

### 1. Retry Mechanism with Exponential Backoff
- **Max attempts**: 4 for POST requests
- **Backoff formula**: `delay = 0.5 * (2 ^ (attempt - 1))`
- **Timeouts**: 5 seconds per request
- **Logging**: Detailed logs show each attempt, failures, and final success/failure

### 2. Deduplication (Exactly Once Delivery)
- UUID is used as primary key
- Hash table lookup ensures no duplicate storage
- Duplicate attempts return status "duplicate"
- Prevents message duplication from retry attempts

### 3. Logging and Observability
- Each service logs with timestamps
- Facade logs retry attempts and outcomes
- Logging service logs storage and deduplication
- Messages service logs GET requests

## Running the Services

### Prerequisites
```bash
pip install -r requirements.txt
```

### Start services (each in separate terminal)

**Terminal 1 - Logging Service:**
```bash
cd micro_basics/logging-service
python main.py
```

**Terminal 2 - Messages Service:**
```bash
cd micro_basics/messages-service
python main.py
```

**Terminal 3 - Facade Service:**
```bash
cd micro_basics/facade-service
python main.py
```

## Running Tests

```bash
# All tests
pytest tests/ -v

# Specific test file
pytest tests/test_logging_service.py -v

# With coverage
pytest tests/ --cov --cov-report=html
```

## Example Usage

### Using curl

**Send a message (POST)**
```bash
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"msg": "Hello, World!"}'
```

Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": {"status": "stored"}
}
```

**Get all messages (GET)**
```bash
curl http://localhost:8000/messages
```

Expected output:
```
Hello, World!
not implemented yet
```

## Message Flow Diagram

### POST Request
1. Client sends POST to facade-service with message
2. Facade generates UUID and forwards to logging-service with retry
3. Logging service stores message or detects duplicate
4. Facade returns response with UUID and status

### GET Request
1. Client sends GET to facade-service
2. Facade queries both logging-service and messages-service
3. Responses are aggregated and returned to client

## Testing Retry Mechanism

To observe the retry mechanism in action:

1. Start all three services
2. Send a POST request:
   ```bash
   curl -X POST http://localhost:8000/messages \
     -H "Content-Type: application/json" \
     -d '{"msg": "Test"}'
   ```
3. While request is in flight, stop logging-service
4. Observe facade logs showing retry attempts
5. Restart logging-service
6. Observe successful delivery after retry

## Testing Deduplication

Deduplication is automatically tested via the retry mechanism:
- Same UUID is reused if request fails and retries
- Logging service detects and rejects duplicates
- Final result shows only one message stored

## Dependencies
- fastapi
- uvicorn[standard]
- httpx
- pydantic
- pytest
- pytest-asyncio

## Project Structure

```
micro_basics/
├── facade-service/
│   └── main.py
├── logging-service/
│   └── main.py
├── messages-service/
│   └── main.py
└── README.md
```

**Performance Testing**
A client is required to perform a specified number of calls to the facade-service and measure the execution time. A simple asynchronous client is provided in this repository at micro_basics/perf/bench.py

Key Features:
- Concurrent Execution: Run N clients simultaneously, each making M requests (totaling N×M requests).
- Metrics Tracking: Measures total wall time, requests per second (RPS), and average/median latency.
- Downstream Analysis: Ability to sample the latency of downstream services (e.g., logging-service or counter-service) to estimate their individual performance impact.

Execution Examples:

```bash
# High load: 10 clients, 10,000 requests each (100k total)
python micro_basics/perf/bench.py --url http://localhost:8000/messages --clients 10 --per-client 10000 --data '{"msg":"1","account":"acc-1"}' --sample-downstream --logging-url http://localhost:8001/log --counter-url http://localhost:8003/counter

# Scenario 1: 10 concurrent clients performing 10k transactions across 10 different accounts
# (Can be automated by running multiple instances with different account IDs)
python micro_basics/perf/bench.py --url http://localhost:8000/messages --clients 10 --per-client 10000 --data '{"msg":"1","account":"acc-<IDX>"}'

# Scenario 2: 10 concurrent clients performing 10k transactions on a single shared account
python micro_basics/perf/bench.py --url http://localhost:8000/messages --clients 10 --per-client 10000 --data '{"msg":"1","account":"shared-acc"}'
```

High load:
![res1](assests/productivity_tests/test_one/img1.png)
![res2](assests/productivity_tests/test_one/img2.png)
![res3](assests/productivity_tests/test_one/img3.png)

Scenario 1:
![res1](assests/productivity_tests/test_two/img1.png)
![res2](assests/productivity_tests/test_two/img2.png)
![res3](assests/productivity_tests/test_two/img3.png)

Scenario 2:
![res1](assests/productivity_tests/test_three/img1.png)
![res2](assests/productivity_tests/test_three/img2.png)
![res3](assests/productivity_tests/test_three/img3.png)


Script Output:
- `total_requests`, `successful_responses`, `total_wall_time_seconds`, `requests_per_second`, та базову статистику латентності.

Estimating Downstream Service Impact:
- The script can send a sample batch (default: 50) of requests directly to the logging-service and counter-service to calculate average latency for each.
- Assuming the facade-service calls both services once per request, the approximate total contribution of these services to the overall time is estimated as: Estimated Impact=avg_latency×total_requests.
