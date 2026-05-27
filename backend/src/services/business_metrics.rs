//! Business metrics service for tracking revenue, costs, and operational KPIs.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{error, info, instrument};
use uuid::Uuid;

use crate::error::AppError;

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricCategory {
    Revenue,
    Costs,
    Users,
    Transactions,
    Performance,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetricSource {
    OnChain,
    OffChain,
    #[default]
    Database,
    ExternalApi,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessMetric {
    pub id: Uuid,
    pub name: String,
    pub value: Decimal,
    pub unit: String,
    pub category: MetricCategory,
    pub tags: HashMap<String, String>,
    pub recorded_at: DateTime<Utc>,
    pub source: MetricSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub total_metrics: i64,
    pub categories: HashMap<String, i64>,
    pub latest_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsQuery {
    pub category: Option<MetricCategory>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub tags: Option<HashMap<String, String>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

pub struct BusinessMetricsService {
    db: PgPool,
    cache: Arc<RwLock<HashMap<String, Vec<BusinessMetric>>>>,
}

impl BusinessMetricsService {
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a new business metric.
    #[instrument(skip(self, tags, value, unit, category, source))]
    pub async fn record_metric(
        &self,
        name: String,
        value: Decimal,
        unit: String,
        category: MetricCategory,
        tags: HashMap<String, String>,
        source: MetricSource,
    ) -> Result<BusinessMetric, AppError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let category_str = serde_json::to_string(&category)
            .map_err(|e| AppError::InternalError(e.to_string()))?;
        let source_str = serde_json::to_string(&source)
            .map_err(|e| AppError::InternalError(e.to_string()))?;
        let tags_json = serde_json::to_value(&tags)
            .map_err(|e| AppError::InternalError(e.to_string()))?;
        // Store Decimal as string to avoid sqlx type issues
        let value_str = value.to_string();

        sqlx::query(
            r#"
            INSERT INTO business_metrics (id, name, value, unit, category, tags, recorded_at, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(id)
        .bind(&name)
        .bind(&value_str)
        .bind(&unit)
        .bind(&category_str)
        .bind(&tags_json)
        .bind(now)
        .bind(&source_str)
        .execute(&self.db)
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to record metric");
            AppError::DatabaseError(e)
        })?;

        let metric = BusinessMetric {
            id,
            name: name.clone(),
            value,
            unit,
            category,
            tags,
            recorded_at: now,
            source,
        };

        // Update in-memory cache
        {
            let mut cache = self.cache.write().await;
            let entry = cache.entry(metric.name.clone()).or_default();
            entry.push(metric.clone());
            if entry.len() > 1000 {
                entry.remove(0);
            }
        }

        info!(
            metric_name = %metric.name,
            value = %metric.value,
            "Recorded business metric"
        );

        Ok(metric)
    }

    /// Remove metrics older than the retention period.
    #[instrument(skip(self))]
    pub async fn prune_old_metrics(&self, retention_days: i64) -> Result<u64, AppError> {
        let cutoff = Utc::now() - Duration::days(retention_days);

        let result = sqlx::query("DELETE FROM business_metrics WHERE recorded_at < $1")
            .bind(cutoff)
            .execute(&self.db)
            .await
            .map_err(|e| AppError::DatabaseError(e))?;

        let deleted = result.rows_affected();
        info!(deleted, retention_days, "Pruned old metrics");
        Ok(deleted)
    }

    /// Get the latest cached value for a metric (no DB call).
    pub async fn get_cached_latest(&self, name: &str) -> Option<BusinessMetric> {
        let cache = self.cache.read().await;
        cache.get(name)?.last().cloned()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_category_serialization() {
        let cat = MetricCategory::Revenue;
        let json = serde_json::to_string(&cat).unwrap();
        assert!(json.contains("revenue"));
    }

    #[test]
    fn test_metric_source_default() {
        let src = MetricSource::default();
        assert_eq!(src, MetricSource::Database);
    }

    #[test]
    fn test_business_metric_serialization() {
        let metric = BusinessMetric {
            id: Uuid::new_v4(),
            name: "revenue".to_string(),
            value: Decimal::new(1000, 2),
            unit: "USD".to_string(),
            category: MetricCategory::Revenue,
            tags: HashMap::from([("region".into(), "us-east".into())]),
            recorded_at: Utc::now(),
            source: MetricSource::Database,
        };
        let json = serde_json::to_string(&metric).unwrap();
        assert!(json.contains("revenue"));
        assert!(json.contains("USD"));
    }

    #[test]
    fn test_metrics_summary_serialization() {
        let summary = MetricsSummary {
            total_metrics: 42,
            categories: HashMap::from([("revenue".into(), 10i64)]),
            latest_timestamp: Some(Utc::now()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("42"));
    }
}
