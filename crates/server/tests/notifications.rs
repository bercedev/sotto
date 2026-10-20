//! Database-backed checks for durable lifecycle notice identity and delivery.
//!
//! These tests are opt-in because they create rows in a disposable local Postgres database.

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Row};
use std::str::FromStr;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};
use tower::ServiceExt;

use sotto_server::cloud_coverage::{evaluate, ConfirmedPaidInterval, PersonCoverage};
use sotto_server::cloud_coverage_store::{self, CoverageProjection};
use sotto_server::config::DeploymentMode;
use sotto_server::db;
use sotto_server::notifications::{
    self, DeliveryOutcome, NoticeChannel, NoticeContent, NoticeError, NoticeIntent, NoticeKind,
    NoticeSender,
};
use sotto_server::state::AppState;

fn notification_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn pool_or_skip() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping notification tests: set SOTTO_RUN_DB_TESTS=1");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for opted-in database tests");
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing notification tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect to test database");
    db::migrate(&pool).await.expect("apply migrations");
    Some(pool)
}

async fn seed_user(pool: &PgPool, user_id: &str) {
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'notification-test', $1) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(user_id)
    .execute(pool)
    .await
    .expect("seed notification user");
}

async fn publish_coverage(
    pool: &PgPool,
    user_id: &str,
    expected_revision: Option<i64>,
    operation_id: &str,
    paid_intervals: Vec<ConfirmedPaidInterval>,
) -> i64 {
    let mut tx = pool.begin().await.expect("begin coverage publication");
    let receipt = cloud_coverage_store::publish(
        &mut tx,
        user_id,
        expected_revision,
        operation_id,
        &format!("evidence-{operation_id}"),
        &CoverageProjection::Complete { paid_intervals },
    )
    .await
    .expect("publish complete coverage");
    tx.commit().await.expect("commit coverage publication");
    receipt.revision
}

async fn publish_unavailable_coverage(
    pool: &PgPool,
    user_id: &str,
    expected_revision: i64,
    operation_id: &str,
) -> i64 {
    let mut tx = pool.begin().await.expect("begin unavailable publication");
    let receipt = cloud_coverage_store::publish(
        &mut tx,
        user_id,
        Some(expected_revision),
        operation_id,
        &format!("evidence-{operation_id}"),
        &CoverageProjection::Unavailable {
            reason: sotto_server::cloud_coverage_store::UnavailableReason::NeedsReconciliation,
        },
    )
    .await
    .expect("publish unavailable coverage");
    tx.commit().await.expect("commit unavailable publication");
    receipt.revision
}

fn eligibility_for(
    coverage: PersonCoverage,
    now_epoch: i64,
) -> sotto_server::person_eligibility::EligibilityView {
    let decision = evaluate(&coverage, now_epoch).expect("evaluate coverage");
    sotto_server::person_eligibility::evaluate_input(
        &sotto_server::person_eligibility::EligibilityInput {
            deployment_mode: DeploymentMode::Cloud,
            billing_available: false,
            account_initialized: true,
            personal_billing_state: None,
            coverage: sotto_server::person_eligibility::CoverageInput::Decision(decision),
        },
    )
}

async fn cleanup(pool: &PgPool, user_id: &str) {
    sqlx::query("DELETE FROM cloud_notice_outbox WHERE recipient_user_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean notification outbox");
    sqlx::query("DELETE FROM cloud_verified_contacts WHERE user_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean notification contacts");
    sqlx::query("DELETE FROM cloud_coverage_heads WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean coverage head");
    sqlx::query("DELETE FROM cloud_coverage_revision_facts WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean coverage facts");
    sqlx::query("DELETE FROM cloud_coverage_revisions WHERE beneficiary_id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean coverage revisions");
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean notification user");
}

fn content() -> NoticeContent {
    NoticeContent {
        title: "Renewal failed".into(),
        detail: "Recovery is available until the displayed deadline.".into(),
        effective_at_epoch: Some(1_800_000_000),
        deadline_epoch: Some(1_800_100_000),
        amount_pence: Some(299),
        price_interval: None,
    }
}

fn in_app_intent(user_id: &str, event_key: &str, due_at_epoch: i64) -> NoticeIntent {
    NoticeIntent {
        recipient_user_id: user_id.into(),
        event_key: event_key.into(),
        policy_key: "renewal-failure-v1".into(),
        kind: NoticeKind::FailedRenewal,
        channel: NoticeChannel::InApp,
        contact_id: None,
        due_at_epoch,
        content: content(),
    }
}

#[tokio::test]
async fn export_window_notice_tracks_the_complete_coverage_episode() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-export-episode";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;

    let now_epoch: i64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let first_end = now_epoch - 18 * 24 * 60 * 60;
    let latest_end = now_epoch - 10 * 24 * 60 * 60;
    let intervals = vec![
        ConfirmedPaidInterval {
            coverage_id: "personal-term-old".into(),
            source_id: "personal-source".into(),
            starts_at: first_end - 30 * 24 * 60 * 60,
            paid_until: first_end,
            failed_renewal_id: None,
        },
        ConfirmedPaidInterval {
            coverage_id: "sponsored-term-latest".into(),
            source_id: "sponsored-source".into(),
            starts_at: latest_end - 30 * 24 * 60 * 60,
            paid_until: latest_end,
            failed_renewal_id: None,
        },
    ];
    let revision = publish_coverage(
        &pool,
        USER_ID,
        None,
        "export-episode-ended",
        intervals.clone(),
    )
    .await;
    let coverage = PersonCoverage {
        beneficiary_id: USER_ID.into(),
        paid_intervals: intervals,
    };
    let view = eligibility_for(coverage.clone(), now_epoch);
    assert_eq!(
        view.state,
        sotto_server::person_eligibility::EligibilityState::ExportOnly
    );
    let deadline = latest_end + sotto_server::cloud_coverage::EXPORT_WINDOW_SECONDS;

    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &view,
        Some(revision),
        now_epoch
    )
    .await
    .expect("record export notice"));
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &view,
        Some(revision),
        now_epoch + 1
    )
    .await
    .expect("replay export notice"));
    assert!(!notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &view,
        Some(revision),
        deadline,
    )
    .await
    .expect("retry eligibility when the deadline passes during the read"));

    let notices = notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("list export notice");
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].kind, "export_deadline");
    assert_eq!(notices[0].content.deadline_epoch, Some(deadline));
    assert_eq!(notices[0].content.effective_at_epoch, Some(latest_end));
    assert_eq!(notices[0].due_at_epoch, latest_end);

    let pending_checkout_view = sotto_server::person_eligibility::evaluate_input(
        &sotto_server::person_eligibility::EligibilityInput {
            deployment_mode: DeploymentMode::Cloud,
            billing_available: true,
            account_initialized: true,
            personal_billing_state: Some(
                sotto_server::personal_billing::PersonalBillingState::Pending,
            ),
            coverage: sotto_server::person_eligibility::CoverageInput::Decision(
                evaluate(&coverage, now_epoch).expect("evaluate current export episode"),
            ),
        },
    );
    assert_eq!(
        pending_checkout_view.state,
        sotto_server::person_eligibility::EligibilityState::PendingInitialPayment
    );
    assert_eq!(pending_checkout_view.export_until_epoch, Some(deadline));
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &pending_checkout_view,
        Some(revision),
        now_epoch + 2
    )
    .await
    .expect("pending checkout preserves the open export episode"));
    let retained_notice: (String, i64) = sqlx::query_as(
        "SELECT status, (payload->>'deadline_epoch')::bigint FROM cloud_notice_outbox \
         WHERE recipient_user_id=$1 AND kind='export_deadline'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("load notice while checkout is pending");
    assert_eq!(retained_notice, ("pending".into(), deadline));

    let unavailable_revision =
        publish_unavailable_coverage(&pool, USER_ID, revision, "export-episode-unavailable").await;
    let unavailable_view = sotto_server::person_eligibility::evaluate_input(
        &sotto_server::person_eligibility::EligibilityInput {
            deployment_mode: DeploymentMode::Cloud,
            billing_available: false,
            account_initialized: true,
            personal_billing_state: None,
            coverage: sotto_server::person_eligibility::CoverageInput::Unavailable,
        },
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &unavailable_view,
        Some(unavailable_revision),
        now_epoch,
    )
    .await
    .expect("unavailable evidence leaves notice unchanged"));
    let unchanged_notice: String = sqlx::query_scalar(
        "SELECT status FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND kind='export_deadline' AND status='pending'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("pending notice survives unavailable evidence");
    assert_eq!(unchanged_notice, "pending");

    let mut recovery_intervals = coverage.paid_intervals.clone();
    recovery_intervals.push(ConfirmedPaidInterval {
        coverage_id: "renewal-recovery-term".into(),
        source_id: "transferred-source".into(),
        starts_at: now_epoch - 30 * 24 * 60 * 60,
        paid_until: now_epoch - 24 * 60 * 60,
        failed_renewal_id: Some("renewal-failed".into()),
    });
    let recovery_revision = publish_coverage(
        &pool,
        USER_ID,
        Some(unavailable_revision),
        "export-episode-renewal-recovery",
        recovery_intervals.clone(),
    )
    .await;
    let recovery_view = eligibility_for(
        PersonCoverage {
            beneficiary_id: USER_ID.into(),
            paid_intervals: recovery_intervals.clone(),
        },
        now_epoch,
    );
    assert_eq!(
        recovery_view.state,
        sotto_server::person_eligibility::EligibilityState::RenewalRecovery
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &recovery_view,
        Some(recovery_revision),
        now_epoch,
    )
    .await
    .expect("supersede export notice during renewal recovery"));
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("list notices during renewal recovery")
        .is_empty());
    let superseded_status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND event_key=$2 AND policy_key='cloud-export-deadline-v1'",
    )
    .bind(USER_ID)
    .bind(format!("cloud-export-window:{latest_end}:{deadline}"))
    .fetch_one(&pool)
    .await
    .expect("load superseded export notice");
    assert_eq!(superseded_status, "cancelled");

    let live_until = now_epoch + 10 * 24 * 60 * 60;
    let mut extended_intervals = recovery_intervals;
    extended_intervals.push(ConfirmedPaidInterval {
        coverage_id: "transfer-term-live".into(),
        source_id: "transferred-source".into(),
        starts_at: now_epoch - 24 * 60 * 60,
        paid_until: live_until,
        failed_renewal_id: None,
    });
    let next_revision = publish_coverage(
        &pool,
        USER_ID,
        Some(recovery_revision),
        "export-episode-extended",
        extended_intervals.clone(),
    )
    .await;
    let paid_view = eligibility_for(
        PersonCoverage {
            beneficiary_id: USER_ID.into(),
            paid_intervals: extended_intervals,
        },
        now_epoch,
    );
    assert_eq!(
        paid_view.state,
        sotto_server::person_eligibility::EligibilityState::Paid
    );

    // A stale view from before the new source was published must not recreate the old notice.
    assert!(!notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &view,
        Some(revision),
        now_epoch
    )
    .await
    .expect("reject stale coverage revision"));
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &paid_view,
        Some(next_revision),
        now_epoch
    )
    .await
    .expect("supersede ended notice while covered"));
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("list notices while paid")
        .is_empty());

    let future_view = eligibility_for(
        PersonCoverage {
            beneficiary_id: USER_ID.into(),
            paid_intervals: paid_view_coverage(&coverage, live_until),
        },
        live_until + 1,
    );
    assert_eq!(
        future_view.state,
        sotto_server::person_eligibility::EligibilityState::ExportOnly
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &future_view,
        Some(next_revision),
        live_until + 1,
    )
    .await
    .expect("record the later export episode"));
    let later_notice: (String, i64, i64) = sqlx::query_as(
        "SELECT status, extract(epoch FROM due_at)::bigint, \
                (payload->>'deadline_epoch')::bigint \
         FROM cloud_notice_outbox WHERE recipient_user_id=$1 AND kind='export_deadline' \
           AND policy_key='cloud-export-deadline-v1' AND status='pending'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("load later scheduled export notice");
    assert_eq!(later_notice.0, "pending");
    assert_eq!(later_notice.1, live_until);
    assert_eq!(later_notice.2, live_until + 30 * 24 * 60 * 60);
    let episode_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND kind='export_deadline' AND policy_key='cloud-export-deadline-v1'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("count durable export episodes");
    // The first expired episode remains as durable evidence; the earlier recovery boundary is
    // superseded and retained as cancelled; the latest complete coverage boundary is pending.
    assert_eq!(episode_count, 3);
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("future export notice is hidden")
        .is_empty());

    // A missing projection is not evidence that the prior export episode was superseded.
    sqlx::query("DELETE FROM cloud_coverage_heads WHERE beneficiary_id=$1")
        .bind(USER_ID)
        .execute(&pool)
        .await
        .expect("remove projection head for missing-evidence case");
    sqlx::query("DELETE FROM cloud_coverage_revision_facts WHERE beneficiary_id=$1")
        .bind(USER_ID)
        .execute(&pool)
        .await
        .expect("remove projection facts for missing-evidence case");
    sqlx::query("DELETE FROM cloud_coverage_revisions WHERE beneficiary_id=$1")
        .bind(USER_ID)
        .execute(&pool)
        .await
        .expect("remove revisions for missing-evidence case");
    let missing_view = sotto_server::person_eligibility::evaluate_input(
        &sotto_server::person_eligibility::EligibilityInput {
            deployment_mode: DeploymentMode::Cloud,
            billing_available: false,
            account_initialized: true,
            personal_billing_state: None,
            coverage: sotto_server::person_eligibility::CoverageInput::Missing,
        },
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &missing_view,
        None,
        live_until + 1,
    )
    .await
    .expect("missing projection preserves notice"));
    let preserved_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND kind='export_deadline' AND status='pending'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("count pending export notices after missing projection");
    assert_eq!(preserved_count, 1);

    cleanup(&pool, USER_ID).await;
}

#[tokio::test]
async fn expired_export_episode_cancels_its_pending_notice_with_an_audit_row() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-export-expired";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;
    let now_epoch: i64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let paid_until = now_epoch - 3 * 24 * 60 * 60;
    let intervals = vec![ConfirmedPaidInterval {
        coverage_id: "expired-export-term".into(),
        source_id: "expired-export-source".into(),
        starts_at: paid_until - 30 * 24 * 60 * 60,
        paid_until,
        failed_renewal_id: None,
    }];
    let revision = publish_coverage(
        &pool,
        USER_ID,
        None,
        "expired-export-episode",
        intervals.clone(),
    )
    .await;
    let coverage = PersonCoverage {
        beneficiary_id: USER_ID.into(),
        paid_intervals: intervals,
    };
    let export_view = eligibility_for(coverage.clone(), now_epoch);
    let deadline = paid_until + sotto_server::cloud_coverage::EXPORT_WINDOW_SECONDS;
    assert_eq!(
        export_view.state,
        sotto_server::person_eligibility::EligibilityState::ExportOnly
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &export_view,
        Some(revision),
        now_epoch,
    )
    .await
    .expect("queue export notice while deadline is open"));
    assert_eq!(
        notifications::list_for_user(&pool, USER_ID)
            .await
            .expect("list export notice before deadline")
            .len(),
        1
    );

    let expired_view = eligibility_for(coverage, deadline);
    assert_eq!(
        expired_view.state,
        sotto_server::person_eligibility::EligibilityState::Expired
    );
    assert!(notifications::reconcile_export_deadline_notice(
        &pool,
        USER_ID,
        &expired_view,
        Some(revision),
        deadline,
    )
    .await
    .expect("cancel notice once the export deadline is reached"));
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("expired notice is no longer presented as current")
        .is_empty());
    let status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_notice_outbox WHERE recipient_user_id=$1 \
         AND kind='export_deadline' AND policy_key='cloud-export-deadline-v1'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("read expired notice audit row");
    assert_eq!(status, "cancelled");
    cleanup(&pool, USER_ID).await;
}

#[tokio::test]
async fn concurrent_coverage_publication_rejects_a_stale_export_notice() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-export-concurrent";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;
    let now_epoch: i64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let ended_at = now_epoch - 24 * 60 * 60;
    let old_intervals = vec![ConfirmedPaidInterval {
        coverage_id: "concurrent-old-term".into(),
        source_id: "concurrent-source".into(),
        starts_at: ended_at - 30 * 24 * 60 * 60,
        paid_until: ended_at,
        failed_renewal_id: None,
    }];
    let old_revision = publish_coverage(
        &pool,
        USER_ID,
        None,
        "concurrent-export-old",
        old_intervals.clone(),
    )
    .await;
    let stale_view = eligibility_for(
        PersonCoverage {
            beneficiary_id: USER_ID.into(),
            paid_intervals: old_intervals.clone(),
        },
        now_epoch,
    );
    assert_eq!(
        stale_view.state,
        sotto_server::person_eligibility::EligibilityState::ExportOnly
    );

    let mut writer = pool.begin().await.expect("begin concurrent publication");
    let holder_pid: i32 = sqlx::query_scalar(
        "SELECT pg_backend_pid() FROM cloud_coverage_heads \
         WHERE beneficiary_id=$1 FOR UPDATE",
    )
    .bind(USER_ID)
    .fetch_one(&mut *writer)
    .await
    .expect("lock coverage head for publication");
    let reconcile_pool = pool.clone();
    let reconcile = tokio::spawn(async move {
        notifications::reconcile_export_deadline_notice(
            &reconcile_pool,
            USER_ID,
            &stale_view,
            Some(old_revision),
            now_epoch,
        )
        .await
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiter_pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity \
             WHERE pid <> pg_backend_pid() AND $1=ANY(pg_blocking_pids(pid)) \
               AND wait_event_type='Lock' AND query ILIKE '%cloud_coverage_heads%' \
               AND query ILIKE '%FOR SHARE%' LIMIT 1",
        )
        .bind(holder_pid)
        .fetch_optional(&pool)
        .await
        .expect("inspect export reconciliation lock wait");
        if waiter_pid.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "export reconciliation did not wait for the coverage head lock"
        );
        sleep(Duration::from_millis(25)).await;
    }

    let mut current_intervals = old_intervals;
    current_intervals.push(ConfirmedPaidInterval {
        coverage_id: "concurrent-new-term".into(),
        source_id: "concurrent-new-source".into(),
        starts_at: now_epoch - 60,
        paid_until: now_epoch + 30 * 24 * 60 * 60,
        failed_renewal_id: None,
    });
    cloud_coverage_store::publish(
        &mut writer,
        USER_ID,
        Some(old_revision),
        "concurrent-export-new",
        "evidence-concurrent-export-new",
        &CoverageProjection::Complete {
            paid_intervals: current_intervals,
        },
    )
    .await
    .expect("publish new source while holding head lock");
    writer
        .commit()
        .await
        .expect("commit concurrent publication");

    assert!(!reconcile
        .await
        .expect("join export notice reconciliation")
        .expect("reconcile stale export view"));
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("no stale notice after concurrent publication")
        .is_empty());
    cleanup(&pool, USER_ID).await;
}

#[tokio::test]
async fn eligibility_route_records_export_notice_without_vault_unlock() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-export-route";
    const OTHER_USER_ID: &str = "notification-test-export-other";
    const CORRUPT_USER_ID: &str = "notification-test-export-corrupt";
    cleanup(&pool, USER_ID).await;
    cleanup(&pool, OTHER_USER_ID).await;
    cleanup(&pool, CORRUPT_USER_ID).await;
    seed_user(&pool, USER_ID).await;
    seed_user(&pool, OTHER_USER_ID).await;
    seed_user(&pool, CORRUPT_USER_ID).await;
    sqlx::query("UPDATE users SET public_key=$2 WHERE id=$1")
        .bind(USER_ID)
        .bind(vec![7_u8; 32])
        .execute(&pool)
        .await
        .expect("mark account as initialised");

    let now_epoch: i64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let paid_until = now_epoch - 3 * 24 * 60 * 60;
    let deadline = paid_until + sotto_server::cloud_coverage::EXPORT_WINDOW_SECONDS;
    publish_coverage(
        &pool,
        USER_ID,
        None,
        "export-route-episode",
        vec![ConfirmedPaidInterval {
            coverage_id: "export-route-term".into(),
            source_id: "export-route-source".into(),
            starts_at: paid_until - 30 * 24 * 60 * 60,
            paid_until,
            failed_renewal_id: None,
        }],
    )
    .await;

    let token = sotto_server::auth::session::issue(&pool, USER_ID)
        .await
        .expect("issue test session");
    let app = Router::new()
        .merge(sotto_server::person_eligibility::router())
        .with_state(AppState {
            pool: pool.clone(),
            deployment_mode: DeploymentMode::Cloud,
            oauth: None,
            oauth_config: None,
            billing: None,
            telemetry_ingest: false,
            contact_verification: None,
            organisation_deletion_enabled: false,
            organisation_deletion_retention_days:
                sotto_server::config::DEFAULT_ORGANISATION_DELETION_RETENTION_DAYS,
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
            cloud_retention_dry_run_enabled: false,
            cloud_retention_operator_token: None,
        });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/account/eligibility")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build eligibility request"),
        )
        .await
        .expect("get eligibility");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("read eligibility response");
    let eligibility: serde_json::Value =
        serde_json::from_slice(&body).expect("decode eligibility response");
    assert_eq!(eligibility["state"], "export_only");
    assert_eq!(eligibility["export_until_epoch"], deadline);
    assert_eq!(eligibility["actions"]["export"], true);

    let notices = notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("list notices after eligibility read");
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].kind, "export_deadline");
    assert_eq!(notices[0].content.deadline_epoch, Some(deadline));
    assert!(notifications::list_for_user(&pool, OTHER_USER_ID)
        .await
        .expect("other recipient cannot see export notice")
        .is_empty());

    let corrupt_paid_until = now_epoch - 24 * 60 * 60;
    let corrupt_revision = publish_coverage(
        &pool,
        CORRUPT_USER_ID,
        None,
        "corrupt-export-route-episode",
        vec![ConfirmedPaidInterval {
            coverage_id: "corrupt-export-term".into(),
            source_id: "corrupt-export-source".into(),
            starts_at: corrupt_paid_until - 30 * 24 * 60 * 60,
            paid_until: corrupt_paid_until,
            failed_renewal_id: None,
        }],
    )
    .await;
    sqlx::query(
        "UPDATE cloud_coverage_revisions SET fact_count=fact_count+1 \
         WHERE beneficiary_id=$1 AND revision=$2",
    )
    .bind(CORRUPT_USER_ID)
    .bind(corrupt_revision)
    .execute(&pool)
    .await
    .expect("corrupt stored projection count");
    let corrupt_token = sotto_server::auth::session::issue(&pool, CORRUPT_USER_ID)
        .await
        .expect("issue corrupt-evidence test session");
    let corrupt_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/account/eligibility")
                .header("authorization", format!("Bearer {corrupt_token}"))
                .body(Body::empty())
                .expect("build corrupt eligibility request"),
        )
        .await
        .expect("request eligibility with corrupt evidence");
    assert_eq!(corrupt_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(notifications::list_for_user(&pool, CORRUPT_USER_ID)
        .await
        .expect("list notices for corrupt account")
        .is_empty());

    cleanup(&pool, USER_ID).await;
    cleanup(&pool, OTHER_USER_ID).await;
    cleanup(&pool, CORRUPT_USER_ID).await;
}

fn paid_view_coverage(previous: &PersonCoverage, live_until: i64) -> Vec<ConfirmedPaidInterval> {
    let mut intervals = previous.paid_intervals.clone();
    intervals.push(ConfirmedPaidInterval {
        coverage_id: "transfer-term-live".into(),
        source_id: "transferred-source".into(),
        starts_at: live_until - 11 * 24 * 60 * 60,
        paid_until: live_until,
        failed_renewal_id: None,
    });
    intervals
}

#[tokio::test]
async fn enqueue_is_idempotent_and_cancelled_notice_is_not_listed() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-idempotency";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;

    let first = notifications::enqueue(&pool, &in_app_intent(USER_ID, "event-1", 1_800_000_000))
        .await
        .expect("enqueue first notice");
    let second = notifications::enqueue(&pool, &in_app_intent(USER_ID, "event-1", 1_900_000_000))
        .await
        .expect("enqueue duplicate notice");
    assert_eq!(first, notifications::EnqueueOutcome::Enqueued);
    assert_eq!(second, notifications::EnqueueOutcome::AlreadyQueued);

    let row = sqlx::query(
        "SELECT extract(epoch FROM due_at)::bigint AS due_at, status, attempt_count \
         FROM cloud_notice_outbox WHERE recipient_user_id=$1 AND event_key='event-1'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("load idempotent notice");
    assert_eq!(row.get::<i64, _>("due_at"), 1_800_000_000);
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert_eq!(row.get::<i32, _>("attempt_count"), 0);

    assert_eq!(
        notifications::cancel(&pool, USER_ID, "event-1", "renewal-failure-v1")
            .await
            .unwrap(),
        1
    );
    assert!(notifications::list_for_user(&pool, USER_ID)
        .await
        .unwrap()
        .is_empty());
    cleanup(&pool, USER_ID).await;
}

#[tokio::test]
async fn account_notice_list_hides_future_due_rows_but_keeps_due_rows() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-due-filter";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;

    let now_epoch: i64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .expect("read database time");
    let due = in_app_intent(USER_ID, "event-due", now_epoch.saturating_sub(1));
    let future = in_app_intent(USER_ID, "event-future", now_epoch.saturating_add(3600));
    notifications::enqueue(&pool, &due)
        .await
        .expect("enqueue due notice");
    notifications::enqueue(&pool, &future)
        .await
        .expect("enqueue future notice");
    // Cancelled rows stay excluded regardless of their due date.
    let cancelled = in_app_intent(USER_ID, "event-cancelled", now_epoch.saturating_sub(1));
    notifications::enqueue(&pool, &cancelled)
        .await
        .expect("enqueue cancellable notice");
    notifications::cancel(&pool, USER_ID, "event-cancelled", "renewal-failure-v1")
        .await
        .expect("cancel due notice");

    let notices = notifications::list_for_user(&pool, USER_ID)
        .await
        .expect("list due notices");
    assert_eq!(notices.len(), 1);
    assert!(notices[0].due_at_epoch <= now_epoch);
    cleanup(&pool, USER_ID).await;
}

struct RetrySender;

#[async_trait]
impl NoticeSender for RetrySender {
    async fn send(&mut self, _delivery: notifications::NoticeDelivery) -> DeliveryOutcome {
        DeliveryOutcome::Retry {
            code: "provider_busy".into(),
        }
    }
}

#[tokio::test]
async fn retry_keeps_lifecycle_due_date_and_records_provider_error() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-retry";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;
    notifications::record_verified_email(
        &pool,
        "contact-retry",
        USER_ID,
        "person@example.test",
        1_800_000_000,
    )
    .await
    .expect("record verified contact");
    let intent = NoticeIntent {
        recipient_user_id: USER_ID.into(),
        event_key: "event-retry".into(),
        policy_key: "price-change-v1".into(),
        kind: NoticeKind::PriceChange,
        channel: NoticeChannel::Email,
        contact_id: Some("contact-retry".into()),
        due_at_epoch: 1,
        content: content(),
    };
    notifications::enqueue(&pool, &intent)
        .await
        .expect("enqueue retry notice");
    let mut sender = RetrySender;
    assert!(notifications::run_once(&pool, "worker-retry", &mut sender)
        .await
        .unwrap());

    let row = sqlx::query(
        "SELECT extract(epoch FROM due_at)::bigint AS due_at, status, attempt_count, last_error_code \
         FROM cloud_notice_outbox WHERE recipient_user_id=$1 AND event_key='event-retry'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("load retried notice");
    assert_eq!(row.get::<i64, _>("due_at"), 1);
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert_eq!(row.get::<i32, _>("attempt_count"), 1);
    assert_eq!(row.get::<String, _>("last_error_code"), "provider_busy");
    cleanup(&pool, USER_ID).await;
}

#[tokio::test]
async fn verified_contact_cannot_be_reassigned_to_another_user() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const OWNER: &str = "notification-test-contact-owner";
    const OTHER: &str = "notification-test-contact-other";
    cleanup(&pool, OWNER).await;
    cleanup(&pool, OTHER).await;
    seed_user(&pool, OWNER).await;
    seed_user(&pool, OTHER).await;
    notifications::record_verified_email(
        &pool,
        "contact-owned",
        OWNER,
        "owner@example.test",
        1_800_000_000,
    )
    .await
    .expect("record owner contact");
    assert!(matches!(
        notifications::record_verified_email(
            &pool,
            "contact-owned",
            OTHER,
            "other@example.test",
            1_800_000_001
        )
        .await,
        Err(NoticeError::InvalidKey("contact belongs to another user"))
    ));
    cleanup(&pool, OWNER).await;
    cleanup(&pool, OTHER).await;
}

#[tokio::test]
async fn deleted_contact_fails_queued_email_without_breaking_the_outbox_row() {
    let _guard = notification_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "notification-test-deleted-contact";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;
    notifications::record_verified_email(
        &pool,
        "contact-deleted",
        USER_ID,
        "person@example.test",
        1_800_000_000,
    )
    .await
    .expect("record verified contact");
    let intent = NoticeIntent {
        recipient_user_id: USER_ID.into(),
        event_key: "event-deleted-contact".into(),
        policy_key: "recovery-v1".into(),
        kind: NoticeKind::RecoveryEnd,
        channel: NoticeChannel::Email,
        contact_id: Some("contact-deleted".into()),
        due_at_epoch: 1,
        content: content(),
    };
    notifications::enqueue(&pool, &intent)
        .await
        .expect("enqueue email notice");
    sqlx::query("DELETE FROM cloud_verified_contacts WHERE contact_id='contact-deleted'")
        .execute(&pool)
        .await
        .expect("delete verified contact");

    let mut sender = RetrySender;
    assert!(
        notifications::run_once(&pool, "worker-deleted-contact", &mut sender)
            .await
            .expect("mark deleted contact unavailable")
    );
    let row = sqlx::query(
        "SELECT status, last_error_code, contact_id FROM cloud_notice_outbox \
         WHERE recipient_user_id=$1 AND event_key='event-deleted-contact'",
    )
    .bind(USER_ID)
    .fetch_one(&pool)
    .await
    .expect("load deleted-contact notice");
    assert_eq!(row.get::<String, _>("status"), "failed");
    assert_eq!(row.get::<String, _>("last_error_code"), "contact_missing");
    assert_eq!(row.get::<Option<String>, _>("contact_id"), None);
    cleanup(&pool, USER_ID).await;
}
