# Crucible Backend

This is the backend service layer for the Crucible toolkit, providing performance profiling, mock service layers, specialized serialization utilities, and robust background monitoring.

## Features

### 🚀 Performance Profiling API
High-performance endpoints for monitoring application health and system metrics.
- `/api/v1/profiling/metrics`: Real-time system metrics.
- `/api/v1/profiling/health`: System health status.
- `/api/status`: Unified health, metrics, and active recovery tasks.

### 🧪 Mock Service Layer
A robust mock layer for testing services in isolation, supporting both database and cache operations.

### 🔢 Custom Serialization
Specialized Serde serializers for high-precision types and Stellar-specific formats.

### 🛠️ Background Services
The backend runs several background workers for system health and data consistency.

| Module | Description |
|---|---|
| `sys_metrics` | Collects and exposes system metrics (CPU, memory, uptime) |
| `error_recovery` | Tracks retry state for failing tasks with configurable max retries |
| `log_aggregator` | Async MPSC-based log pipeline; persists entries via a background worker |
| `log_alerts` | Threshold-based alerting over the log pipeline with sliding-window evaluation |
| `feature_flags` | Feature flag management backed by PostgreSQL with Redis caching |

## Tech Stack
- **Web Framework**: Axum (async Rust)
- **Runtime**: Tokio
- **Database**: PostgreSQL (via SQLx 0.8)
- **Caching & Jobs**: Redis (via Apalis)
- **Serialization**: Serde
- **Observability**: Tracing
- **API Documentation**: Utoipa (Swagger UI)

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Base API greeting |
| `GET` | `/.well-known/stellar.toml` | Stellar network metadata |
| `GET` | `/api/v1/profiling/metrics` | Detailed performance metrics (OpenAPI) |
| `GET` | `/api/v1/profiling/health` | Service health check (OpenAPI) |
| `GET` | `/api/status` | System health summary and recovery status |
| `POST` | `/api/profile` | Trigger a manual profiling collection run |
| `GET` | `/swagger-ui` | Interactive API documentation |

## Development

### Running the App
```bash
cargo run -p backend
```

### Backup Service
A small HTTP binary for triggering logical backups of the Postgres `public` schema and storing the result in Redis.

Environment variables:
- `DATABASE_URL` — Postgres connection string
- `REDIS_URL` — Redis connection string
- `BACKUP_BIND` — (optional) bind address, default `127.0.0.1:3002`

Endpoints:
- `POST /backup` — trigger a backup; returns a `job_id` (202 Accepted)
- `GET /backup/{job_id}` — query status and fetch JSON backup when complete

Run the server locally:
```bash
DATABASE_URL=postgres://user:pass@localhost/db REDIS_URL=redis://127.0.0.1/ cargo run -p backend --bin backup
```

### Running Tests
```bash
# All tests (unit + integration)
cargo test -p backend

# Load tests specifically
cargo test -p backend --test load_tests -- --nocapture
```

## Structure
- `src/api/` – API handlers and routing
- `src/config/` – Environment configuration
- `src/db/` – Database utilities and seed data
- `src/jobs/` – Background job definitions (Apalis)
- `src/services/` – Business logic and external integrations
- `src/telemetry/` – Observability and logging setup

