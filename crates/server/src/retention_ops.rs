//! Protected, redacted inspection and non-destructive controls for personal expiry dry runs.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::header::CACHE_CONTROL;
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::error::{Error, Result};
use crate::org_deletion_metrics::{bearer_token, token_matches};
use crate::retention::{self, OperatorAction, RetentionError};
use crate::state::AppState;

const MAX_PAGE_SIZE: i64 = 100;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/ops/cloud/retention", get(list_jobs))
        .route("/ops/cloud/retention/{fingerprint}", post(apply_action))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    after: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct StateCount {
    state: &'static str,
    count: i64,
}

#[derive(Debug, Serialize)]
struct HoldCount {
    reason: &'static str,
    count: i64,
}

#[derive(Debug, Serialize)]
struct JobSummary {
    fingerprint: String,
    state: &'static str,
    deadline_epoch: i64,
    dry_run_completed_at_epoch: Option<i64>,
    planned_items: i64,
    dry_run_items: i64,
    held_items: i64,
    hold_reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    states: Vec<StateCount>,
    holds: Vec<HoldCount>,
    jobs: Vec<JobSummary>,
    next_cursor: Option<String>,
}

async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Response> {
    require_operator(&state, &headers)?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::BadRequest("retention page limit is invalid".into()));
    }
    if query
        .after
        .as_deref()
        .is_some_and(|cursor| !is_fingerprint(cursor))
    {
        return Err(Error::BadRequest("retention cursor is invalid".into()));
    }
    let state_rows = sqlx::query(
        "SELECT state, count(*) AS count FROM cloud_retention_jobs \
         WHERE scope_kind='personal' GROUP BY state ORDER BY state",
    )
    .fetch_all(&state.pool)
    .await?;
    let states = state_rows
        .into_iter()
        .filter_map(|row| {
            let state: String = row.try_get("state").ok()?;
            Some(StateCount {
                state: safe_state(&state),
                count: row.try_get("count").ok()?,
            })
        })
        .collect();
    let hold_rows = sqlx::query(
        "SELECT reason, sum(count) AS count FROM ( \
           SELECT hold_code AS reason, count(*) AS count FROM cloud_retention_jobs \
             WHERE scope_kind='personal' AND hold_code IS NOT NULL GROUP BY hold_code \
           UNION ALL \
           SELECT item.hold_code AS reason, count(*) AS count \
             FROM cloud_retention_scope_items item JOIN cloud_retention_jobs job USING(job_id) \
             WHERE job.scope_kind='personal' AND item.state='held' AND item.hold_code IS NOT NULL \
             GROUP BY item.hold_code \
         ) holds GROUP BY reason ORDER BY reason",
    )
    .fetch_all(&state.pool)
    .await?;
    let holds = hold_rows
        .into_iter()
        .filter_map(|row| {
            let reason: Option<String> = row.try_get("reason").ok()?;
            Some(HoldCount {
                reason: safe_reason(reason.as_deref()?),
                count: row.try_get("count").ok()?,
            })
        })
        .collect();
    let mut rows = sqlx::query(
        "SELECT j.operator_fingerprint, j.state, \
                extract(epoch from j.deadline_at)::bigint AS deadline_epoch, \
                extract(epoch from j.dry_run_completed_at)::bigint AS dry_run_completed_at_epoch, \
                j.hold_code, \
                count(*) FILTER (WHERE i.state='planned') AS planned_items, \
                count(*) FILTER (WHERE i.state='dry_run') AS dry_run_items, \
                count(*) FILTER (WHERE i.state='held') AS held_items \
         FROM cloud_retention_jobs j LEFT JOIN cloud_retention_scope_items i USING(job_id) \
         WHERE j.scope_kind='personal' AND ($1::text IS NULL OR j.operator_fingerprint COLLATE \"C\" > $1 COLLATE \"C\") \
         GROUP BY j.job_id ORDER BY j.operator_fingerprint COLLATE \"C\" LIMIT $2",
    )
    .bind(query.after.as_deref())
    .bind(limit + 1)
    .fetch_all(&state.pool)
    .await?;
    let has_more = rows.len() as i64 > limit;
    if has_more {
        rows.truncate(limit as usize);
    }
    let jobs = rows
        .into_iter()
        .map(|row| {
            Ok(JobSummary {
                fingerprint: row.try_get("operator_fingerprint")?,
                state: safe_state(&row.try_get::<String, _>("state")?),
                deadline_epoch: row.try_get("deadline_epoch")?,
                dry_run_completed_at_epoch: row.try_get("dry_run_completed_at_epoch")?,
                planned_items: row.try_get("planned_items")?,
                dry_run_items: row.try_get("dry_run_items")?,
                held_items: row.try_get("held_items")?,
                hold_reason: row
                    .try_get::<Option<String>, _>("hold_code")?
                    .as_deref()
                    .map(safe_reason),
            })
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
    let next_cursor = if has_more {
        jobs.last().map(|job| job.fingerprint.clone())
    } else {
        None
    };
    no_store(Json(ListResponse {
        states,
        holds,
        jobs,
        next_cursor,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionRequest {
    operator: String,
    action: ActionKind,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActionKind {
    Hold,
    Cancel,
    RetryDryRun,
}

async fn apply_action(
    State(state): State<AppState>,
    Path(fingerprint): Path<String>,
    headers: HeaderMap,
    body: std::result::Result<Json<ActionRequest>, JsonRejection>,
) -> Result<Response> {
    require_operator(&state, &headers)?;
    if !is_fingerprint(&fingerprint) {
        return Err(Error::BadRequest(
            "retention job reference is invalid".into(),
        ));
    }
    let Json(body) = body.map_err(|_| Error::BadRequest("invalid retention action".into()))?;
    if body.operator.trim().is_empty()
        || body.operator.len() > 80
        || body.operator.chars().any(char::is_control)
    {
        return Err(Error::BadRequest("operator label is invalid".into()));
    }
    let action = match body.action {
        ActionKind::Hold => OperatorAction::Hold,
        ActionKind::Cancel => OperatorAction::Cancel,
        ActionKind::RetryDryRun => OperatorAction::RetryDryRun,
    };
    let job_id: Option<String> = sqlx::query_scalar(
        "SELECT job_id FROM cloud_retention_jobs \
         WHERE operator_fingerprint=$1 AND scope_kind='personal'",
    )
    .bind(&fingerprint)
    .fetch_optional(&state.pool)
    .await?;
    let Some(job_id) = job_id else {
        return Err(Error::NotFound("retention job not found".into()));
    };
    retention::apply_operator_action(&state.pool, &job_id, &body.operator, action)
        .await
        .map_err(map_retention_error)?;
    no_store(Json(serde_json::json!({ "accepted": true })))
}

fn require_operator(state: &AppState, headers: &HeaderMap) -> Result<()> {
    let expected = state
        .cloud_retention_operator_token
        .as_deref()
        .ok_or_else(|| {
            Error::NotConfigured("Cloud retention operator controls are disabled".into())
        })?;
    let provided = bearer_token(headers).ok_or(Error::Unauthorized)?;
    if !token_matches(expected, provided) {
        return Err(Error::Unauthorized);
    }
    Ok(())
}

fn no_store(body: impl IntoResponse) -> Result<Response> {
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

fn is_fingerprint(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_state(value: &str) -> &'static str {
    match value {
        "planned" => "planned",
        "dry_run" => "dry_run",
        "ready" => "ready",
        "leased" => "leased",
        "held" => "held",
        "cancelled" => "cancelled",
        "completed" => "completed",
        "failed" => "failed",
        _ => "unknown",
    }
}

fn safe_reason(value: &str) -> &'static str {
    match value {
        "operator_hold" => "operator_hold",
        "operator_cancelled" => "operator_cancelled",
        "eligibility_unavailable" => "eligibility_unavailable",
        "coverage_resumed" => "coverage_resumed",
        "reconciliation_failed" => "reconciliation_failed",
        "coverage_revision_changed" => "coverage_revision_changed",
        "coverage_revision_missing" => "coverage_revision_missing",
        "coverage_unavailable" => "coverage_unavailable",
        "coverage_episode_changed" => "coverage_episode_changed",
        "coverage_episode_unproven" => "coverage_episode_unproven",
        "coverage_deadline_missing" => "coverage_deadline_missing",
        "coverage_deadline_invalid" => "coverage_deadline_invalid",
        "billing_state_active" => "billing_state_active",
        "notice_evidence_missing" => "notice_evidence_missing",
        "scope_exceeds_bound" => "scope_exceeds_bound",
        "scope_changed" => "scope_changed",
        "share_lifetime_unproven" => "share_lifetime_unproven",
        "shared_resource" => "shared_resource",
        "resource_missing" => "resource_missing",
        "resource_identity_changed" => "resource_identity_changed",
        "unsupported_scope" => "unsupported_scope",
        "scope_owner_missing" => "scope_owner_missing",
        "episode_superseded" => "episode_superseded",
        _ => "other",
    }
}

fn map_retention_error(error: RetentionError) -> Error {
    match error {
        RetentionError::Database(error) => Error::Db(error),
        RetentionError::NotFound => {
            Error::Conflict("retention action is not valid for this job".into())
        }
        RetentionError::LeaseLost => Error::Conflict("retention job is currently changing".into()),
        _ => Error::BadRequest("retention action is invalid".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_output_labels_are_fixed_and_fingerprints_are_bounded() {
        assert_eq!(safe_reason("secret provider payload"), "other");
        assert_eq!(safe_state("secret state"), "unknown");
        assert!(is_fingerprint("0123456789abcdef0123456789abcdef"));
        assert!(!is_fingerprint("user-secret"));
        assert!(!is_fingerprint("0123456789ABCDEF0123456789ABCDEF"));
    }
}
