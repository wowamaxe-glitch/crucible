//! Health check endpoints.
//!
//! Provides two endpoints:
//!
//! - `GET /health/live`  — liveness probe: returns 200 if the process is running.
//! - `GET /health/ready` — readiness probe: returns 200 only when PostgreSQL and
//!   Redis are reachable; returns 503 otherwise.
//!
//! Both endpoints return a JSON body with per-component status details so that
//! operators can quickly identify which dependency is unhealthy.

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::Serialize;
use sqlx::PgPool;
use tracing::{debug, instrument, warn};

/// Minimal application state required by health check handlers.
#[derive(Clone)]
pub struct HealthState {
    pub db: PgPool,
    pub redis: ConnectionManager,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Status of a single dependency.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ComponentStatus {
    Healthy,
    Unhealthy,
}

/// Response body for the readiness probe.
#[derive(Debug, Serialize)]
pub struct ReadinessResponse {
    /// Overall status: `"healthy"` or `"degraded"`.
    pub status: String,
    /// PostgreSQL connectivity.
    pub database: ComponentStatus,
    /// Redis connectivity.
    pub cache: ComponentStatus,
    /// Application version from `CARGO_PKG_VERSION`.
    pub version: String,
}

/// Response body for the liveness probe.
#[derive(Debug, Serialize)]
pub struct LivenessResponse {
    pub status: &'static str,
    pub version: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /health/live` — liveness probe.
///
/// Always returns `200 OK` as long as the process is running. Kubernetes uses
/// this to decide whether to restart the container.
#[instrument(skip_all)]
pub async fn liveness() -> impl IntoResponse {
    debug!("Liveness probe");
    (
        StatusCode::OK,
        Json(LivenessResponse {
            status: "ok",
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )
}

/// `GET /health/ready` — readiness probe.
///
/// Checks PostgreSQL and Redis connectivity. Returns `200 OK` when all
/// dependencies are healthy, or `503 Service Unavailable` when any are not.
/// Kubernetes uses this to decide whether to route traffic to the pod.
#[instrument(skip_all)]
pub async fn readiness(State(state): State<HealthState>) -> impl IntoResponse {
    let db_status = check_database(&state).await;
    let cache_status = check_cache(&state).await;

    let all_healthy =
        db_status == ComponentStatus::Healthy && cache_status == ComponentStatus::Healthy;

    let status_code = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status_code,
        Json(ReadinessResponse {
            status: if all_healthy { "healthy".into() } else { "degraded".into() },
            database: db_status,
            cache: cache_status,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    )
}

// ---------------------------------------------------------------------------
// Dependency checks
// ---------------------------------------------------------------------------

async fn check_database(state: &HealthState) -> ComponentStatus {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.db)
        .await
    {
        Ok(_) => {
            debug!("Database health check passed");
            ComponentStatus::Healthy
        }
        Err(e) => {
            warn!("Database health check failed: {e}");
            ComponentStatus::Unhealthy
        }
    }
}

async fn check_cache(state: &HealthState) -> ComponentStatus {
    let mut conn = state.redis.clone();
    match redis::cmd("PING").query_async::<String>(&mut conn).await {
        Ok(_) => {
            debug!("Cache health check passed");
            ComponentStatus::Healthy
        }
        Err(e) => {
            warn!("Cache health check failed: {e}");
            ComponentStatus::Unhealthy
        }
    }
}

// ---------------------------------------------------------------------------
// Router helper
// ---------------------------------------------------------------------------

/// Returns an Axum router with the health check routes mounted.
///
/// Mount this under `/health` in the main application router:
///
/// ```rust,no_run
/// use axum::Router;
/// use backend::api::handlers::health;
///
/// let app: Router = Router::new()
///     .nest("/health", health::router());
/// ```
pub fn router() -> axum::Router<HealthState> {
    use axum::routing::get;
    axum::Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(readiness))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    /// Build a minimal router with only the liveness endpoint (no AppState needed).
    fn liveness_app() -> axum::Router {
        use axum::routing::get;
        axum::Router::new().route("/live", get(liveness))
    }

    #[tokio::test]
    async fn liveness_returns_200() {
        let app = liveness_app();
        let response = app
            .oneshot(Request::builder().uri("/live").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn liveness_body_contains_ok() {
        let app = liveness_app();
        let response = app
            .oneshot(Request::builder().uri("/live").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json["version"].is_string());
    }

    #[test]
    fn readiness_response_serializes_healthy() {
        let resp = ReadinessResponse {
            status: "healthy".into(),
            database: ComponentStatus::Healthy,
            cache: ComponentStatus::Healthy,
            version: "0.1.0".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["database"], "healthy");
        assert_eq!(json["cache"], "healthy");
    }

    #[test]
    fn readiness_response_serializes_degraded() {
        let resp = ReadinessResponse {
            status: "degraded".into(),
            database: ComponentStatus::Unhealthy,
            cache: ComponentStatus::Healthy,
            version: "0.1.0".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "degraded");
        assert_eq!(json["database"], "unhealthy");
        assert_eq!(json["cache"], "healthy");
    }

    #[test]
    fn component_status_eq() {
        assert_eq!(ComponentStatus::Healthy, ComponentStatus::Healthy);
        assert_ne!(ComponentStatus::Healthy, ComponentStatus::Unhealthy);
    }
}
