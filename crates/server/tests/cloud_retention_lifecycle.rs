//! PostgreSQL coverage for the personal expiry candidate and non-destructive dry-run path.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use sha2::Digest;
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Row};
use std::str::FromStr;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tower::ServiceExt;

use sotto_server::cloud_coverage::{ConfirmedPaidInterval, EXPORT_WINDOW_SECONDS};
use sotto_server::cloud_coverage_reconciliation::{
    begin_collection, finish_collection_with_authority, register_source, AuthorityReceiptInput,
    SourceBinding, SourceObservation,
};
use sotto_server::config::DeploymentMode;
use sotto_server::db;
use sotto_server::retention::{self, EnqueueOutcome, RetentionIntent, ScopeKind};
use sotto_server::state::AppState;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn pool_or_skip() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping cloud retention lifecycle tests: set SOTTO_RUN_DB_TESTS=1");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for opted-in database tests");
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing cloud retention lifecycle tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect to test database");
    db::migrate(&pool).await.expect("apply migrations");
    Some(pool)
}

fn state(pool: PgPool) -> AppState {
    AppState {
        pool,
        deployment_mode: DeploymentMode::Cloud,
        oauth: None,
        oauth_config: None,
        billing: None,
        telemetry_ingest: false,
        contact_verification: None,
        organisation_deletion_enabled: false,
        organisation_deletion_retention_days: 30,
        organisation_deletion_metrics_token: None,
        organisation_deletion_operator_token: None,
        billing_transfer_operator_token: None,
        cloud_action_enforcement_enabled: false,
        cloud_coverage_authority_enforcement_enabled: false,
        cloud_coverage_authority_shadow_enabled: false,
        machine_eligibility_enforcement_enabled: false,
        billing_transfers_enabled: false,
        billing_transfer_execution_enabled: false,
        billing_transfer_setup_enabled: false,
        cloud_retention_dry_run_enabled: true,
        cloud_retention_operator_token: Some("0123456789abcdef0123456789abcdef".into()),
    }
}

async fn seed_user(pool: &PgPool, user_id: &str) {
    sqlx::query(
        "INSERT INTO users(id,oauth_provider,oauth_subject) VALUES($1,'retention-test',$1)",
    )
    .bind(user_id)
    .execute(pool)
    .await
    .expect("seed user");
}

async fn seed_expired_projection(pool: &PgPool, user_id: &str, now: i64) -> (i64, i64) {
    let export_start = now - EXPORT_WINDOW_SECONDS - 120;
    let deadline = export_start + EXPORT_WINDOW_SECONDS;
    seed_authoritative_source(pool, user_id).await;
    publish_authoritative_coverage(
        pool,
        user_id,
        vec![ConfirmedPaidInterval {
            coverage_id: format!("coverage:{user_id}"),
            source_id: format!("source:{user_id}"),
            starts_at: export_start - 86_400,
            paid_until: export_start,
            failed_renewal_id: None,
        }],
        now,
        "expired",
    )
    .await;
    (export_start, deadline)
}

async fn seed_authoritative_source(pool: &PgPool, user_id: &str) {
    let source = SourceBinding {
        beneficiary_id: user_id.to_owned(),
        source_id: format!("source:{user_id}"),
        provider_namespace: "stripe".into(),
        external_allocation_reference: format!("allocation-reference:{user_id}"),
        ownership_evidence_reference: format!("ownership:{user_id}"),
    };
    let payer_id = format!("payer:{user_id}");
    let mut tx = pool.begin().await.expect("begin authority source setup");
    register_source(&mut tx, &format!("register:{user_id}"), &source)
        .await
        .expect("register authority source");
    sqlx::query(
        "INSERT INTO cloud_provider_payers \
         (payer_id, provider_namespace, provider_account_id, provider_environment, provider_customer_id, payer_kind) \
         VALUES ($1, 'stripe', 'acct_retention_test', 'test', $2, 'personal')",
    )
    .bind(&payer_id)
    .bind(format!("customer:{user_id}"))
    .execute(&mut *tx)
    .await
    .expect("insert authority payer");
    sqlx::query(
        "INSERT INTO cloud_provider_allocations \
         (allocation_id, payer_id, beneficiary_id, provider_namespace, provider_account_id, \
          provider_environment, provider_subscription_id, provider_item_id, external_allocation_reference, \
          coverage_source_id, effective_from, effective_until, state, ownership_evidence_reference) \
         VALUES ($1, $2, $3, 'stripe', 'acct_retention_test', 'test', $4, $5, $6, $7, 0, $8, 'ended', $9)",
    )
    .bind(format!("allocation:{user_id}"))
    .bind(&payer_id)
    .bind(user_id)
    .bind(format!("subscription:{user_id}"))
    .bind(format!("item:{user_id}"))
    .bind(&source.external_allocation_reference)
    .bind(&source.source_id)
    .bind(1_i64)
    .bind(&source.ownership_evidence_reference)
    .execute(&mut *tx)
    .await
    .expect("insert authority allocation");
    tx.commit().await.expect("commit authority source setup");
}

async fn publish_authoritative_coverage(
    pool: &PgPool,
    user_id: &str,
    paid_intervals: Vec<ConfirmedPaidInterval>,
    now: i64,
    suffix: &str,
) {
    let source_id = format!("source:{user_id}");
    let source_material = format!("{}:{}\nnone\n", source_id.len(), source_id);
    let correction_digest = format!("{:x}", sha2::Sha256::digest(source_material.as_bytes()));
    let attempt_id = format!("scheduled-revalidation:{user_id}:{suffix}");
    let mut tx = pool.begin().await.expect("begin authoritative collection");
    let ticket = begin_collection(&mut tx, user_id, &attempt_id)
        .await
        .expect("prepare authoritative collection");
    let authority = AuthorityReceiptInput {
        provider_namespace: "stripe".into(),
        provider_account_id: "acct_retention_test".into(),
        provider_environment: "test".into(),
        observed_at_epoch: now - 60,
        fresh_until_epoch: now + 90 * 24 * 60 * 60,
        policy_version: sotto_server::cloud_coverage_authority::POLICY_VERSION,
        local_correction_digest: correction_digest,
    };
    finish_collection_with_authority(
        &mut tx,
        &ticket,
        &format!("retention-evidence:{user_id}:{suffix}"),
        &[SourceObservation::Complete {
            source_id,
            evidence_reference: format!("provider-evidence:{user_id}:{suffix}"),
            paid_intervals,
        }],
        Some(&authority),
    )
    .await
    .expect("publish authoritative projection");
    tx.commit().await.expect("commit authoritative projection");
    assert!(matches!(
        sotto_server::cloud_coverage_authority::load_current(pool, user_id, now)
            .await
            .expect("load seeded authority"),
        sotto_server::cloud_coverage_authority::AuthorityState::Current { .. }
    ));
}

async fn seed_notice(pool: &PgPool, user_id: &str, start: i64, deadline: i64, created_at: i64) {
    let event_key = format!("cloud-export-window:{start}:{deadline}");
    let payload = serde_json::json!({
        "title": "Your Cloud account is in its export period",
        "detail": "Download an encrypted export before your export deadline.",
        "effective_at_epoch": start,
        "deadline_epoch": deadline,
        "amount_pence": null,
    });
    sqlx::query(
        "INSERT INTO cloud_notice_outbox \
         (notice_id,recipient_user_id,event_key,policy_key,kind,channel,payload,due_at,available_at,created_at) \
         VALUES($1,$2,$3,'cloud-export-deadline-v1','export_deadline','in_app',$4,to_timestamp($5),to_timestamp($6),to_timestamp($7))",
    )
    .bind(format!("notice:{user_id}"))
    .bind(user_id)
    .bind(event_key)
    .bind(payload)
    .bind(start)
    .bind(start)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("seed timely in-app notice");
}

async fn cleanup(pool: &PgPool, user_id: &str, organisation_id: &str) {
    sqlx::query(
        "DELETE FROM cloud_retention_operator_actions WHERE job_id IN \
         (SELECT job_id FROM cloud_retention_jobs WHERE scope_key=$1)",
    )
    .bind(user_id)
    .execute(pool)
    .await
    .expect("remove operator audit rows");
    sqlx::query("DELETE FROM cloud_retention_jobs WHERE scope_key=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove retention jobs");
    sqlx::query("DELETE FROM cloud_notice_outbox WHERE recipient_user_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove notices");
    sqlx::query("DELETE FROM cloud_coverage_authority_receipts WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove authority receipts");
    sqlx::query(
        "UPDATE cloud_coverage_coordinators SET current_attempt_id = NULL WHERE beneficiary_id=$1",
    )
    .bind(user_id)
    .execute(pool)
    .await
    .expect("clear current coverage attempt");
    sqlx::query("DELETE FROM cloud_coverage_collection_attempts WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove collection attempts");
    sqlx::query("DELETE FROM cloud_provider_allocations WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove provider allocations");
    sqlx::query("DELETE FROM cloud_coverage_source_retirements WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove source retirements");
    sqlx::query("DELETE FROM cloud_coverage_sources WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove coverage sources");
    sqlx::query("DELETE FROM cloud_coverage_revalidation_jobs WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove revalidation job");
    sqlx::query("DELETE FROM cloud_coverage_coordinators WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove coverage coordinator");
    sqlx::query("DELETE FROM cloud_coverage_heads WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove coverage head");
    sqlx::query("DELETE FROM cloud_coverage_revision_facts WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove coverage facts");
    sqlx::query("DELETE FROM cloud_coverage_revisions WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove coverage revisions");
    sqlx::query("DELETE FROM cloud_provider_payers WHERE payer_id=$1")
        .bind(format!("payer:{user_id}"))
        .execute(pool)
        .await
        .expect("remove provider payer");
    sqlx::query("DELETE FROM projects WHERE owner_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove projects");
    sqlx::query("DELETE FROM share_links WHERE created_by=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove share links");
    sqlx::query("DELETE FROM organizations WHERE id=$1")
        .bind(organisation_id)
        .execute(pool)
        .await
        .expect("remove test organisation");
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("remove test user");
}

#[tokio::test]
async fn expiry_dry_run_snapshots_only_personal_data_and_never_deletes_rows() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let suffix = uuid::Uuid::new_v4();
    let user_id = format!("retention-lifecycle-user-{suffix}");
    let other_user_id = format!("retention-lifecycle-other-{suffix}");
    let organisation_id = format!("retention-lifecycle-org-{suffix}");
    let project_id = format!("retention-lifecycle-project-{suffix}");
    let shared_personal_project_id =
        format!("retention-lifecycle-shared-personal-project-{suffix}");
    let shared_project_id = format!("retention-lifecycle-shared-project-{suffix}");
    let environment_id = format!("retention-lifecycle-env-{suffix}");
    let shared_personal_environment_id =
        format!("retention-lifecycle-shared-personal-env-{suffix}");
    let shared_environment_id = format!("retention-lifecycle-shared-env-{suffix}");
    let safe_share_id = format!("retention-lifecycle-share-safe-{suffix}");
    let legacy_share_id = format!("retention-lifecycle-share-legacy-{suffix}");
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    cleanup(&pool, &other_user_id, &format!("unused-{suffix}")).await;
    seed_user(&pool, &user_id).await;
    seed_user(&pool, &other_user_id).await;
    let (export_start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    sqlx::query("INSERT INTO projects(id,owner_id,enc_name) VALUES($1,$2,decode('01','hex'))")
        .bind(&project_id)
        .bind(&user_id)
        .execute(&pool)
        .await
        .expect("seed personal project");
    sqlx::query(
        "INSERT INTO environments(id,project_id,enc_name,revision) \
         VALUES($1,$2,decode('01','hex'),3)",
    )
    .bind(&environment_id)
    .bind(&project_id)
    .execute(&pool)
    .await
    .expect("seed personal environment");
    sqlx::query(
        "INSERT INTO environment_grants(env_id,user_id,enc_vault_key) \
         VALUES($1,$2,decode('03','hex'))",
    )
    .bind(&environment_id)
    .bind(&user_id)
    .execute(&pool)
    .await
    .expect("seed owner's environment grant");
    sqlx::query("INSERT INTO projects(id,owner_id,enc_name) VALUES($1,$2,decode('06','hex'))")
        .bind(&shared_personal_project_id)
        .bind(&user_id)
        .execute(&pool)
        .await
        .expect("seed shared personal project");
    sqlx::query(
        "INSERT INTO environments(id,project_id,enc_name,revision) \
         VALUES($1,$2,decode('07','hex'),2)",
    )
    .bind(&shared_personal_environment_id)
    .bind(&shared_personal_project_id)
    .execute(&pool)
    .await
    .expect("seed shared personal environment");
    sqlx::query(
        "INSERT INTO environment_grants(env_id,user_id,enc_vault_key,granted_by) \
         VALUES($1,$2,decode('08','hex'),$3)",
    )
    .bind(&shared_personal_environment_id)
    .bind(&other_user_id)
    .bind(&user_id)
    .execute(&pool)
    .await
    .expect("share personal environment with another user");
    sqlx::query("INSERT INTO organizations(id,enc_name) VALUES($1,decode('04','hex'))")
        .bind(&organisation_id)
        .execute(&pool)
        .await
        .expect("seed organisation");
    sqlx::query(
        "INSERT INTO projects(id,owner_id,org_id,enc_name) \
         VALUES($1,$2,$3,decode('05','hex'))",
    )
    .bind(&shared_project_id)
    .bind(&user_id)
    .bind(&organisation_id)
    .execute(&pool)
    .await
    .expect("seed organisation-owned project");
    sqlx::query(
        "INSERT INTO environments(id,project_id,enc_name,revision) \
         VALUES($1,$2,decode('01','hex'),1)",
    )
    .bind(&shared_environment_id)
    .bind(&shared_project_id)
    .execute(&pool)
    .await
    .expect("seed organisation-owned environment");
    sqlx::query(
        "INSERT INTO share_links(id,token,enc_blob,created_by,max_views,expires_at,share_class) \
         VALUES($1,$2,decode('01','hex'),$3,1,to_timestamp($4),'free'), \
               ($5,$6,decode('02','hex'),$3,1,NULL,'legacy')",
    )
    .bind(&safe_share_id)
    .bind(format!("token-safe-{suffix}"))
    .bind(&user_id)
    .bind(deadline - 1)
    .bind(&legacy_share_id)
    .bind(format!("token-legacy-{suffix}"))
    .execute(&pool)
    .await
    .expect("seed safe and legacy shares");

    let state = state(pool.clone());
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("reconcile expiry candidates");
    let job_id: String = sqlx::query_scalar(
        "SELECT job_id FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load held expiry job before notice evidence");
    let initial_state: String =
        sqlx::query_scalar("SELECT state FROM cloud_retention_jobs WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("read initial job state");
    assert_eq!(initial_state, "held");

    // The notice producer can race the candidate scanner. A later pass must attach verified
    // notice evidence and release only the corresponding missing-notice hold. Expiry itself
    // cancels the current notice row after the window; its timestamp proves it was available at
    // the start, so the retention evidence must remain usable without leaving it in the inbox.
    seed_notice(&pool, &user_id, export_start, deadline, export_start - 60).await;
    sqlx::query(
        "UPDATE cloud_notice_outbox SET status='cancelled', cancelled_at=to_timestamp($2) \
         WHERE recipient_user_id=$1",
    )
    .bind(&user_id)
    .bind(now)
    .execute(&pool)
    .await
    .expect("expire the notice after its availability window");
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("attach later notice evidence");
    let job = sqlx::query(
        "SELECT job_id, state, purge_enabled, \
                extract(epoch from notice_evidence_at)::bigint AS notice_evidence_epoch, \
                operator_fingerprint \
         FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load expiry job");
    assert_eq!(job.try_get::<String, _>("job_id").expect("job id"), job_id);
    assert_eq!(job.get::<String, _>("state"), "ready");
    assert!(!job.get::<bool, _>("purge_enabled"));
    assert_eq!(
        job.get::<Option<i64>, _>("notice_evidence_epoch"),
        Some(export_start)
    );
    let fingerprint: String = job.try_get("operator_fingerprint").expect("fingerprint");
    assert_eq!(fingerprint.len(), 32);

    let scope = sqlx::query(
        "SELECT resource_kind,resource_id,state,hold_code,ownership_kind,expected_revision \
         FROM cloud_retention_scope_items WHERE job_id=$1",
    )
    .bind(&job_id)
    .fetch_all(&pool)
    .await
    .expect("load scope");
    assert!(scope.iter().any(|row| {
        row.get::<String, _>("resource_kind") == "project"
            && row.get::<String, _>("resource_id") == project_id
            && row.get::<String, _>("state") == "planned"
    }));
    assert!(scope.iter().any(|row| {
        row.get::<String, _>("resource_kind") == "environment"
            && row.get::<String, _>("resource_id") == environment_id
            && row.get::<Option<i64>, _>("expected_revision") == Some(3)
    }));
    assert!(!scope.iter().any(|row| {
        row.get::<String, _>("resource_id") == shared_project_id
            || row.get::<String, _>("resource_id") == shared_environment_id
    }));
    assert!(scope.iter().any(|row| {
        row.get::<String, _>("resource_id") == shared_personal_project_id
            && row.get::<String, _>("state") == "held"
            && row.get::<Option<String>, _>("hold_code").as_deref() == Some("shared_resource")
    }));
    assert!(scope.iter().any(|row| {
        row.get::<String, _>("resource_id") == shared_personal_environment_id
            && row.get::<String, _>("state") == "held"
            && row.get::<Option<String>, _>("hold_code").as_deref() == Some("shared_resource")
    }));
    assert!(scope.iter().any(|row| {
        row.get::<String, _>("resource_id") == legacy_share_id
            && row.get::<String, _>("state") == "held"
            && row.get::<String, _>("hold_code") == "share_lifetime_unproven"
    }));

    let run = sotto_server::retention::run_dry_run_once(&pool, "retention-lifecycle-worker")
        .await
        .expect("run dry run")
        .expect("due job claimed");
    assert_eq!(run.state.as_str(), "dry_run");
    assert_eq!(run.held_items, 3);
    let job = sqlx::query(
        "SELECT state,purge_enabled,(dry_run_completed_at IS NOT NULL) AS completed \
         FROM cloud_retention_jobs WHERE job_id=$1",
    )
    .bind(&job_id)
    .fetch_one(&pool)
    .await
    .expect("reload dry-run job");
    assert_eq!(job.get::<String, _>("state"), "dry_run");
    assert!(!job.get::<bool, _>("purge_enabled"));
    assert!(job.get::<bool, _>("completed"));
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_retention_receipts WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("count receipts");
    let tombstone_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_retention_tombstone_journal WHERE job_id=$1",
    )
    .bind(&job_id)
    .fetch_one(&pool)
    .await
    .expect("count tombstones");
    assert_eq!(receipt_count, 0);
    assert_eq!(tombstone_count, 0);
    let resources_remain: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1) \
         AND EXISTS(SELECT 1 FROM environments WHERE id=$2) \
         AND EXISTS(SELECT 1 FROM share_links WHERE id=$3) \
         AND EXISTS(SELECT 1 FROM share_links WHERE id=$4) \
         AND EXISTS(SELECT 1 FROM projects WHERE id=$5) \
         AND EXISTS(SELECT 1 FROM environments WHERE id=$6)",
    )
    .bind(&project_id)
    .bind(&environment_id)
    .bind(&safe_share_id)
    .bind(&legacy_share_id)
    .bind(&shared_personal_project_id)
    .bind(&shared_personal_environment_id)
    .fetch_one(&pool)
    .await
    .expect("check original rows remain");
    assert!(resources_remain);

    let operator = sotto_server::retention_ops::router().with_state(state);
    let unauthorised = operator
        .clone()
        .oneshot(
            Request::get("/ops/cloud/retention")
                .body(Body::empty())
                .expect("build unauthorised operator request"),
        )
        .await
        .expect("unauthorised operator response");
    assert_eq!(unauthorised.status(), StatusCode::UNAUTHORIZED);
    let response = operator
        .clone()
        .oneshot(
            Request::get("/ops/cloud/retention?limit=10")
                .header("authorization", "Bearer 0123456789abcdef0123456789abcdef")
                .body(Body::empty())
                .expect("build operator list request"),
        )
        .await
        .expect("operator list response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("read operator list");
    let body = String::from_utf8(body.to_vec()).expect("operator JSON is UTF-8");
    assert!(body.contains(&fingerprint));
    assert!(!body.contains(&user_id));
    assert!(!body.contains(&project_id));
    assert!(!body.contains(&safe_share_id));

    for action in ["hold", "retry_dry_run", "cancel"] {
        let response = operator
            .clone()
            .oneshot(
                Request::post(format!("/ops/cloud/retention/{fingerprint}"))
                    .header("authorization", "Bearer 0123456789abcdef0123456789abcdef")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        "{{\"operator\":\"retention reviewer\",\"action\":\"{action}\"}}"
                    )))
                    .expect("build operator action request"),
            )
            .await
            .expect("operator action response");
        assert_eq!(response.status(), StatusCode::OK, "action {action}");
    }
    let operator_actions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_retention_operator_actions WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("count operator actions");
    let purge_enabled: bool =
        sqlx::query_scalar("SELECT purge_enabled FROM cloud_retention_jobs WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("read purge flag after operator controls");
    assert_eq!(operator_actions, 3);
    assert!(!purge_enabled);
    cleanup(&pool, &user_id, &organisation_id).await;
    cleanup(&pool, &other_user_id, &format!("unused-{suffix}")).await;
}

#[tokio::test]
async fn new_paid_coverage_cancels_a_prepared_expiry_episode() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let user_id = format!("retention-resumed-user-{}", uuid::Uuid::new_v4());
    let organisation_id = format!("retention-resumed-org-{}", uuid::Uuid::new_v4());
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    seed_user(&pool, &user_id).await;
    let (start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    seed_notice(&pool, &user_id, start, deadline, start - 60).await;
    let state = state(pool.clone());
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("enqueue expired episode");
    let job_id: String = sqlx::query_scalar(
        "SELECT job_id FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load queued job");
    publish_authoritative_coverage(
        &pool,
        &user_id,
        vec![ConfirmedPaidInterval {
            coverage_id: format!("coverage:{user_id}:renewed"),
            source_id: format!("source:{user_id}"),
            starts_at: now - 60,
            paid_until: now + 30 * 24 * 60 * 60,
            failed_renewal_id: None,
        }],
        now,
        "renewed",
    )
    .await;
    let run = sotto_server::retention::run_dry_run_once(&pool, "retention-resumed-worker")
        .await
        .expect("recheck candidate against new coverage")
        .expect("candidate claimed");
    assert_eq!(run.state.as_str(), "held");
    let hold: Option<String> =
        sqlx::query_scalar("SELECT hold_code FROM cloud_retention_jobs WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("read coverage-change hold");
    assert_eq!(hold.as_deref(), Some("coverage_revision_changed"));
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("cancel superseded expiry");
    let state: String =
        sqlx::query_scalar("SELECT state FROM cloud_retention_jobs WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("read cancelled job");
    assert_eq!(state, "cancelled");
    let notice_event_key = format!(
        "cloud-export-window:{}:{}",
        now + 30 * 24 * 60 * 60,
        now + 60 * 24 * 60 * 60
    );
    let notice_was_prepared: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND event_key=$2 AND kind='export_deadline' AND channel='in_app' \
         AND due_at > created_at)",
    )
    .bind(&user_id)
    .bind(notice_event_key)
    .fetch_one(&pool)
    .await
    .expect("verify future export notice was made available ahead of the window");
    assert!(notice_was_prepared);
    cleanup(&pool, &user_id, &organisation_id).await;
}

#[tokio::test]
async fn invalidated_authority_holds_a_prepared_expiry_episode() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let user_id = format!("retention-authority-stale-{}", uuid::Uuid::new_v4());
    let organisation_id = format!("retention-authority-org-{}", uuid::Uuid::new_v4());
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    seed_user(&pool, &user_id).await;
    let (start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    seed_notice(&pool, &user_id, start, deadline, start - 60).await;
    let state = state(pool.clone());
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("enqueue expired episode");
    let job_id: String = sqlx::query_scalar(
        "SELECT job_id FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load queued job");
    sqlx::query(
        "UPDATE cloud_coverage_coordinators \
         SET provider_invalidation_generation = provider_invalidation_generation + 1 \
         WHERE beneficiary_id=$1",
    )
    .bind(&user_id)
    .execute(&pool)
    .await
    .expect("invalidate the authority without changing the projection");

    let run = sotto_server::retention::run_dry_run_once(&pool, "retention-authority-worker")
        .await
        .expect("hold the candidate when its authority is stale")
        .expect("candidate claimed");
    assert_eq!(run.state.as_str(), "held");
    let (hold, completed): (Option<String>, bool) = sqlx::query_as(
        "SELECT hold_code, dry_run_completed_at IS NOT NULL \
         FROM cloud_retention_jobs WHERE job_id=$1",
    )
    .bind(&job_id)
    .fetch_one(&pool)
    .await
    .expect("read authority hold");
    assert_eq!(hold.as_deref(), Some("coverage_authority_unavailable"));
    assert!(!completed);
    cleanup(&pool, &user_id, &organisation_id).await;
}

#[tokio::test]
async fn late_export_notice_keeps_the_expiry_candidate_held() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let user_id = format!("retention-late-notice-user-{}", uuid::Uuid::new_v4());
    let organisation_id = format!("retention-late-notice-org-{}", uuid::Uuid::new_v4());
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    seed_user(&pool, &user_id).await;
    let (start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    seed_notice(&pool, &user_id, start, deadline, start + 1).await;
    let state = state(pool.clone());
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("reconcile expiry with late notice");

    let candidate = sqlx::query(
        "SELECT state, hold_code, notice_evidence_at IS NULL AS evidence_missing \
         FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load held candidate");
    assert_eq!(candidate.get::<String, _>("state"), "held");
    assert_eq!(
        candidate.get::<Option<String>, _>("hold_code").as_deref(),
        Some("notice_evidence_missing")
    );
    assert!(candidate.get::<bool, _>("evidence_missing"));
    cleanup(&pool, &user_id, &organisation_id).await;
}

#[tokio::test]
async fn changed_environment_revision_is_held_without_deletion() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let suffix = uuid::Uuid::new_v4();
    let user_id = format!("retention-drift-user-{suffix}");
    let organisation_id = format!("retention-drift-org-{suffix}");
    let project_id = format!("retention-drift-project-{suffix}");
    let late_project_id = format!("retention-drift-late-project-{suffix}");
    let environment_id = format!("retention-drift-env-{suffix}");
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    seed_user(&pool, &user_id).await;
    let (start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    seed_notice(&pool, &user_id, start, deadline, start - 60).await;
    sqlx::query("INSERT INTO projects(id,owner_id,enc_name) VALUES($1,$2,decode('01','hex'))")
        .bind(&project_id)
        .bind(&user_id)
        .execute(&pool)
        .await
        .expect("seed project");
    sqlx::query(
        "INSERT INTO environments(id,project_id,enc_name,revision) \
         VALUES($1,$2,decode('02','hex'),3)",
    )
    .bind(&environment_id)
    .bind(&project_id)
    .execute(&pool)
    .await
    .expect("seed environment");
    let state = state(pool.clone());
    sotto_server::cloud_retention_lifecycle::reconcile_user(&state, &user_id)
        .await
        .expect("enqueue expiry candidate");
    let job_id: String = sqlx::query_scalar(
        "SELECT job_id FROM cloud_retention_jobs WHERE scope_key=$1 AND scope_kind='personal'",
    )
    .bind(&user_id)
    .fetch_one(&pool)
    .await
    .expect("load candidate job");
    sqlx::query("UPDATE environments SET revision=revision+1 WHERE id=$1")
        .bind(&environment_id)
        .execute(&pool)
        .await
        .expect("simulate environment drift after snapshot");

    let run = sotto_server::retention::run_dry_run_once(&pool, "retention-drift-worker")
        .await
        .expect("run dry run")
        .expect("candidate claimed");
    assert_eq!(run.state.as_str(), "held");
    let (hold, completed): (Option<String>, bool) = sqlx::query_as(
        "SELECT hold_code, dry_run_completed_at IS NOT NULL \
         FROM cloud_retention_jobs WHERE job_id=$1",
    )
    .bind(&job_id)
    .fetch_one(&pool)
    .await
    .expect("read held candidate");
    assert_eq!(hold.as_deref(), Some("resource_identity_changed"));
    assert!(!completed);

    // A new personal project created after the immutable snapshot must be detected on a later
    // operator retry rather than silently left outside the candidate.
    sqlx::query("INSERT INTO projects(id,owner_id,enc_name) VALUES($1,$2,decode('03','hex'))")
        .bind(&late_project_id)
        .bind(&user_id)
        .execute(&pool)
        .await
        .expect("add project after candidate snapshot");
    sotto_server::retention::apply_operator_action(
        &pool,
        &job_id,
        "retention reviewer",
        sotto_server::retention::OperatorAction::RetryDryRun,
    )
    .await
    .expect("retry held dry run");
    let retry = sotto_server::retention::run_dry_run_once(&pool, "retention-drift-worker")
        .await
        .expect("recheck scope inventory")
        .expect("candidate claimed for retry");
    assert_eq!(retry.state.as_str(), "held");
    let hold: Option<String> =
        sqlx::query_scalar("SELECT hold_code FROM cloud_retention_jobs WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("read scope-change hold");
    assert_eq!(hold.as_deref(), Some("scope_changed"));
    let remains: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1) \
         AND EXISTS(SELECT 1 FROM environments WHERE id=$2) \
         AND EXISTS(SELECT 1 FROM projects WHERE id=$3)",
    )
    .bind(&project_id)
    .bind(&environment_id)
    .bind(&late_project_id)
    .fetch_one(&pool)
    .await
    .expect("confirm drifted resources remain");
    assert!(remains);
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_retention_receipts WHERE job_id=$1")
            .bind(&job_id)
            .fetch_one(&pool)
            .await
            .expect("confirm no receipts");
    assert_eq!(receipts, 0);
    cleanup(&pool, &user_id, &organisation_id).await;
}

#[tokio::test]
async fn bounded_scan_is_dark_until_enabled_and_persists_its_cursor() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let user_id = format!("!retention-scan-user-{}", uuid::Uuid::new_v4());
    let organisation_id = format!("retention-scan-org-{}", uuid::Uuid::new_v4());
    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    cleanup(&pool, &user_id, &organisation_id).await;
    seed_user(&pool, &user_id).await;
    let (start, deadline) = seed_expired_projection(&pool, &user_id, now).await;
    seed_notice(&pool, &user_id, start, deadline, start - 60).await;
    let mut state = state(pool.clone());
    state.cloud_retention_dry_run_enabled = false;
    assert_eq!(
        sotto_server::cloud_retention_lifecycle::reconcile_once(&state)
            .await
            .expect("disabled scan is a no-op"),
        0
    );
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_retention_jobs WHERE scope_key=$1")
            .bind(&user_id)
            .fetch_one(&pool)
            .await
            .expect("confirm disabled scan created no job");
    assert_eq!(before, 0);

    state.cloud_retention_dry_run_enabled = true;
    let scanned = sotto_server::cloud_retention_lifecycle::reconcile_once(&state)
        .await
        .expect("run bounded expiry scan");
    assert!(scanned > 0);
    let job_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM cloud_retention_jobs WHERE scope_key=$1)")
            .bind(&user_id)
            .fetch_one(&pool)
            .await
            .expect("confirm scanner produced the expiry candidate");
    assert!(job_exists);
    let scan_state: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT cursor_user_id, lease_owner FROM cloud_retention_scan_state \
         WHERE scan_key='personal_expiry'",
    )
    .fetch_one(&pool)
    .await
    .expect("read released scan lease");
    assert!(scan_state.1.is_none());
    if scanned == 32 {
        assert!(scan_state.0.is_some());
    } else {
        assert!(scan_state.0.is_none());
    }
    cleanup(&pool, &user_id, &organisation_id).await;
}

#[tokio::test]
async fn bounded_scan_holds_one_conflicted_user_and_continues_to_later_users() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let suffix = uuid::Uuid::new_v4();
    let failed_user = format!("!retention-scan-a-{suffix}");
    let later_user = format!("!retention-scan-z-{suffix}");
    let failed_org = format!("retention-scan-a-org-{suffix}");
    let later_org = format!("retention-scan-z-org-{suffix}");
    cleanup(&pool, &failed_user, &failed_org).await;
    cleanup(&pool, &later_user, &later_org).await;
    sqlx::query(
        "INSERT INTO cloud_retention_scan_state(scan_key,cursor_user_id,lease_owner,lease_expires_at) \
         VALUES('personal_expiry',NULL,NULL,NULL) ON CONFLICT(scan_key) DO UPDATE SET \
         cursor_user_id=NULL,lease_owner=NULL,lease_expires_at=NULL",
    )
    .execute(&pool)
    .await
    .expect("reset the bounded scan cursor");

    let now: i64 = sqlx::query_scalar("SELECT extract(epoch from now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    seed_user(&pool, &failed_user).await;
    seed_user(&pool, &later_user).await;
    let (failed_start, failed_deadline) = seed_expired_projection(&pool, &failed_user, now).await;
    let (later_start, later_deadline) = seed_expired_projection(&pool, &later_user, now).await;
    seed_notice(
        &pool,
        &failed_user,
        failed_start,
        failed_deadline,
        failed_start - 60,
    )
    .await;
    seed_notice(
        &pool,
        &later_user,
        later_start,
        later_deadline,
        later_start - 60,
    )
    .await;
    let failed_revision: i64 = sqlx::query_scalar(
        "SELECT current_revision FROM cloud_coverage_heads WHERE beneficiary_id=$1",
    )
    .bind(&failed_user)
    .fetch_one(&pool)
    .await
    .expect("read user's current revision");
    let stale_intent = RetentionIntent {
        job_id: format!("retention:stale-identity-{suffix}"),
        scope_kind: ScopeKind::Personal,
        scope_key: failed_user.clone(),
        subject_user_id: Some(failed_user.clone()),
        organisation_id: None,
        eligibility_episode: format!("export-expiry:{failed_start}:{failed_deadline}"),
        deadline_epoch: failed_deadline - 1,
        notice_event_key: format!("cloud-export-window:{failed_start}:{failed_deadline}"),
        notice_evidence_epoch: Some(failed_start),
        expected_coverage_revision: Some(failed_revision),
    };
    assert_eq!(
        retention::enqueue(&pool, &stale_intent)
            .await
            .expect("seed one conflicting immutable job identity"),
        EnqueueOutcome::Enqueued
    );

    let state = state(pool.clone());
    let scanned = sotto_server::cloud_retention_lifecycle::reconcile_once(&state)
        .await
        .expect("one user conflict must not abort the bounded scan");
    assert!(scanned >= 2, "scan should reach both fixture users");
    let held: (String, Option<String>) =
        sqlx::query_as("SELECT state,hold_code FROM cloud_retention_jobs WHERE scope_key=$1")
            .bind(&failed_user)
            .fetch_one(&pool)
            .await
            .expect("read held conflicting candidate");
    assert_eq!(held.0, "held");
    assert_eq!(held.1.as_deref(), Some("reconciliation_failed"));
    let later_job_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM cloud_retention_jobs WHERE scope_key=$1)")
            .bind(&later_user)
            .fetch_one(&pool)
            .await
            .expect("read candidate after the failing user");
    assert!(later_job_exists, "the scan must continue to the next user");

    cleanup(&pool, &failed_user, &failed_org).await;
    cleanup(&pool, &later_user, &later_org).await;
    sqlx::query(
        "UPDATE cloud_retention_scan_state SET cursor_user_id=NULL,lease_owner=NULL,lease_expires_at=NULL \
         WHERE scan_key='personal_expiry'",
    )
    .execute(&pool)
    .await
    .expect("restore the scan cursor");
}
