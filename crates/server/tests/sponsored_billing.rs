//! Database coverage for named sponsor seat intents.
//!
//! These tests are opt-in like the other PostgreSQL integration targets. They use unique fixture
//! identities and remove the operation rows explicitly because their foreign keys preserve history.

use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::db;
use sotto_server::sponsored_billing::{
    begin_operation, cancel_claimed_expired_checkout, cancel_claimed_rejected_operation,
    claim_recovery, complete_paid_checkout, next_recovery_candidate, persist_provider_request,
    record_checkout, record_provider_update, record_recovery_failure, SponsoredBillingError,
    SponsoredProviderEvidence, SponsoredProviderRequest, SponsoredSeatAction, SponsoredSeatRequest,
};

async fn pool() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping sponsored billing tests: SOTTO_RUN_DB_TESTS=1 not set");
        return None;
    }
    let url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping sponsored billing tests: DATABASE_URL not set");
            return None;
        }
    };
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing sponsored billing tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect");
    db::migrate(&pool).await.expect("migrate");
    Some(pool)
}

#[tokio::test]
async fn named_seat_operation_is_idempotent_and_activates_after_paid_result() {
    let Some(pool) = pool().await else {
        return;
    };
    let suffix = Uuid::new_v4().to_string();
    let org_id = format!("sponsored-org-{suffix}");
    let owner = format!("sponsored-owner-{suffix}");
    let beneficiary = format!("sponsored-beneficiary-{suffix}");
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES \
         ($1, 'sponsored-test', $1), ($2, 'sponsored-test', $2)",
    )
    .bind(&owner)
    .bind(&beneficiary)
    .execute(&pool)
    .await
    .expect("insert users");
    sqlx::query(
        "INSERT INTO organizations (id, enc_name, created_by) \
         VALUES ($1, decode('6f7267', 'hex'), $2)",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert organisation");
    sqlx::query(
        "INSERT INTO organization_memberships (org_id, user_id, role) VALUES ($1, $2, 'owner')",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert owner membership");

    let request = SponsoredSeatRequest {
        action: SponsoredSeatAction::Add,
        beneficiary_id: beneficiary.clone(),
        replacement_beneficiary_id: None,
        offer: BillingOffer::StandardMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 4_000_000_000,
        effective_from: 1_700_000_001,
        effective_until: None,
        idempotency_key: "seat-add-1".into(),
    };
    let mut tx = pool.begin().await.expect("begin operation transaction");
    let first = begin_operation(&mut tx, &org_id, &owner, &request, 1_700_000_001)
        .await
        .expect("create operation");
    tx.commit().await.expect("commit operation");

    let mut tx = pool.begin().await.expect("begin replay transaction");
    let replay = begin_operation(&mut tx, &org_id, &owner, &request, 1_700_000_001)
        .await
        .expect("replay operation");
    tx.commit().await.expect("commit replay");
    assert_eq!(first.operation_id, replay.operation_id);

    let mut tx = pool.begin().await.expect("begin late replay transaction");
    let late_replay = begin_operation(&mut tx, &org_id, &owner, &request, 5_000_000_000)
        .await
        .expect("late replay keeps the original operation recoverable");
    tx.commit().await.expect("commit late replay");
    assert_eq!(first.operation_id, late_replay.operation_id);

    let mut tx = pool.begin().await.expect("begin checkout transaction");
    record_checkout(
        &mut tx,
        &first.operation_id,
        "https://stripe.test/checkout",
        "cs_sponsored_test",
        Some("cus_sponsored_test"),
        Some("sub_sponsored_test"),
    )
    .await
    .expect("record checkout");
    tx.commit().await.expect("commit checkout");

    let mut tx = pool.begin().await.expect("begin settlement transaction");
    let evidence = SponsoredProviderEvidence {
        customer_id: Some("cus_sponsored_test".into()),
        subscription_id: "sub_sponsored_test".into(),
        schedule_id: None,
        checkout_session_id: Some("cs_sponsored_test".into()),
        payment_reference: "pi_sponsored_test".into(),
        provider_item_id: Some("si_sponsored_test".into()),
    };
    sqlx::query(
        "UPDATE billing_sponsored_operations SET state = 'manual_review', \
         result_code = 'provider_idempotency_window_elapsed', \
         recovery_manual_review_required = TRUE, \
         recovery_last_error_code = 'provider_idempotency_window_elapsed' \
         WHERE operation_id = $1",
    )
    .bind(&first.operation_id)
    .execute(&mut *tx)
    .await
    .expect("park an unresolved provider result for operator review");
    let settled = complete_paid_checkout(&mut tx, &first.operation_id, &evidence)
        .await
        .expect("verified payment settles even while recovery is parked");
    tx.commit().await.expect("commit settlement");
    assert_eq!(settled.state, "active");
    assert!(!settled.recovery_manual_review_required);
    let state: String =
        sqlx::query_scalar("SELECT state FROM billing_sponsored_seats WHERE operation_id = $1")
            .bind(&first.operation_id)
            .fetch_one(&pool)
            .await
            .expect("load seat state");
    assert_eq!(state, "active");
    let provider_item: Option<String> = sqlx::query_scalar(
        "SELECT provider_item_id FROM billing_sponsored_seats WHERE operation_id = $1",
    )
    .bind(&first.operation_id)
    .fetch_one(&pool)
    .await
    .expect("load provider item");
    assert_eq!(provider_item.as_deref(), Some("si_sponsored_test"));
    let stored_provider_ids: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT provider_customer_id, provider_subscription_id \
         FROM billing_sponsored_subscriptions WHERE organization_id = $1",
    )
    .bind(&org_id)
    .fetch_one(&pool)
    .await
    .expect("load sponsored subscription");
    assert_eq!(stored_provider_ids.0.as_deref(), Some("cus_sponsored_test"));
    assert_eq!(stored_provider_ids.1.as_deref(), Some("sub_sponsored_test"));

    let removal_request = SponsoredSeatRequest {
        action: SponsoredSeatAction::Remove,
        beneficiary_id: beneficiary.clone(),
        replacement_beneficiary_id: None,
        offer: BillingOffer::StandardMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 4_000_000_000,
        effective_from: 1_800_000_000,
        effective_until: Some(1_800_000_000 + 1),
        idempotency_key: "seat-remove-1".into(),
    };
    let mut tx = pool.begin().await.expect("begin removal transaction");
    let removal = begin_operation(&mut tx, &org_id, &owner, &removal_request, 1_700_000_001)
        .await
        .expect("create removal operation");
    tx.commit().await.expect("commit removal operation");
    let mut tx = pool.begin().await.expect("begin provider update");
    record_provider_update(
        &mut tx,
        &removal.operation_id,
        Some("cus_sponsored_test"),
        "sub_sponsored_test",
        Some("sch_sponsored_test"),
        Some(1_700_000_100),
        None,
    )
    .await
    .expect("record provider update");
    tx.commit().await.expect("commit provider update");
    let removal_state: (String, Option<i64>) = sqlx::query_as(
        "SELECT state, effective_until FROM billing_sponsored_seats WHERE operation_id = $1",
    )
    .bind(&first.operation_id)
    .fetch_one(&pool)
    .await
    .expect("load scheduled removal");
    assert_eq!(removal_state.0, "scheduled_removal");
    assert_eq!(removal_state.1, Some(1_800_000_001));
    let schedule_id: Option<String> = sqlx::query_scalar(
        "SELECT provider_schedule_id FROM billing_sponsored_subscriptions WHERE organization_id = $1",
    )
    .bind(&org_id)
    .fetch_one(&pool)
    .await
    .expect("load stored schedule");
    assert_eq!(schedule_id.as_deref(), Some("sch_sponsored_test"));

    let mut tx = pool.begin().await.expect("begin conflicting settlement");
    let mut conflicting_evidence = evidence.clone();
    conflicting_evidence.payment_reference = "pi_other".into();
    let conflict = complete_paid_checkout(&mut tx, &first.operation_id, &conflicting_evidence)
        .await
        .expect_err("a different provider result must conflict");
    assert!(matches!(conflict, SponsoredBillingError::ResultConflict));
    tx.rollback()
        .await
        .expect("rollback conflicting settlement");

    sqlx::query("UPDATE organizations SET lifecycle_state = 'deleting' WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("mark organisation deleting");
    let mut tx = pool.begin().await.expect("begin deleting settlement");
    let deletion_error = complete_paid_checkout(&mut tx, &first.operation_id, &evidence)
        .await
        .expect_err("a deleting organisation cannot settle another webhook");
    assert!(matches!(
        deletion_error,
        SponsoredBillingError::OrganisationNotActive
    ));
    tx.rollback().await.expect("rollback deleting settlement");
    sqlx::query("UPDATE organizations SET lifecycle_state = 'active' WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("restore organisation lifecycle");

    sqlx::query("DELETE FROM billing_sponsored_seats WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete seats");
    sqlx::query("DELETE FROM billing_sponsored_operations WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete operations");
    sqlx::query("DELETE FROM billing_sponsored_subscription_items WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete subscription items");
    sqlx::query("DELETE FROM billing_sponsored_subscriptions WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete sponsored subscription");
    sqlx::query("DELETE FROM organization_memberships WHERE org_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete membership");
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete organisation");
    sqlx::query("DELETE FROM users WHERE id IN ($1, $2)")
        .bind(&owner)
        .bind(&beneficiary)
        .execute(&pool)
        .await
        .expect("delete users");
}

#[tokio::test]
async fn sponsored_recovery_replays_immutable_request_under_a_fenced_lease() {
    let Some(pool) = pool().await else {
        return;
    };
    let suffix = Uuid::new_v4().to_string();
    let org_id = format!("sponsored-recovery-org-{suffix}");
    let owner = format!("sponsored-recovery-owner-{suffix}");
    let beneficiary = format!("sponsored-recovery-beneficiary-{suffix}");
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES \
         ($1, 'sponsored-recovery-test', $1), ($2, 'sponsored-recovery-test', $2)",
    )
    .bind(&owner)
    .bind(&beneficiary)
    .execute(&pool)
    .await
    .expect("insert users");
    sqlx::query(
        "INSERT INTO organizations (id, enc_name, created_by) \
         VALUES ($1, decode('6f7267', 'hex'), $2)",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert organisation");
    sqlx::query(
        "INSERT INTO organization_memberships (org_id, user_id, role) \
         VALUES ($1, $2, 'owner')",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert owner membership");

    let request = SponsoredSeatRequest {
        action: SponsoredSeatAction::Add,
        beneficiary_id: beneficiary.clone(),
        replacement_beneficiary_id: None,
        offer: BillingOffer::StandardMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 4_000_000_000,
        effective_from: 1_700_000_001,
        effective_until: None,
        idempotency_key: "recovery-add-1".into(),
    };
    let mut tx = pool.begin().await.expect("begin sponsored operation");
    let operation = begin_operation(&mut tx, &org_id, &owner, &request, 1_700_000_001)
        .await
        .expect("create operation");
    tx.commit().await.expect("commit operation");
    assert_eq!(
        next_recovery_candidate(&pool)
            .await
            .expect("read recovery candidates"),
        None,
        "an operation without a persisted provider request is not claimable"
    );

    let provider_request = SponsoredProviderRequest::Checkout {
        organization_id: org_id.clone(),
        customer_id: Some("cus_saved".into()),
        items: vec![sotto_server::billing::SponsoredSubscriptionItemRequest {
            provider_price_id: "price_saved".into(),
            quantity: 1,
        }],
        expires_at_epoch: 1_700_003_600,
        success_url: "https://example.test/success".into(),
        cancel_url: "https://example.test/cancel".into(),
    };
    let mut tx = pool.begin().await.expect("begin request save");
    persist_provider_request(&mut tx, &operation.operation_id, &provider_request)
        .await
        .expect("persist exact request before provider I/O");
    tx.commit().await.expect("commit request");
    assert_eq!(
        next_recovery_candidate(&pool)
            .await
            .expect("read prepared recovery candidate")
            .as_deref(),
        Some(operation.operation_id.as_str())
    );

    let claimed = claim_recovery(&pool, &operation.operation_id, "claim:first")
        .await
        .expect("claim operation")
        .expect("operation is due");
    assert_eq!(claimed.recovery_attempt_count, 0);
    assert!(claimed.provider_first_sent_epoch.is_some());
    assert_eq!(claimed.provider_request_version, Some(1));
    assert!(
        claim_recovery(&pool, &operation.operation_id, "claim:second")
            .await
            .expect("second claim check")
            .is_none()
    );

    // An expired owner cannot record a provider response after another worker takes the lease.
    sqlx::query(
        "UPDATE billing_sponsored_operations SET recovery_lease_until = now() - interval '1 second', \
         recovery_next_attempt_at = now() WHERE operation_id = $1",
    )
    .bind(&operation.operation_id)
    .execute(&pool)
    .await
    .expect("expire first lease");
    let reclaimed = claim_recovery(&pool, &operation.operation_id, "claim:second")
        .await
        .expect("reclaim operation")
        .expect("expired lease is reclaimable");
    assert_eq!(reclaimed.recovery_attempt_count, 0);
    record_recovery_failure(
        &pool,
        &operation.operation_id,
        "claim:second",
        "provider_retryable",
    )
    .await
    .expect("record one provider failure");
    sqlx::query(
        "UPDATE billing_sponsored_operations SET recovery_next_attempt_at = now() \
         WHERE operation_id = $1",
    )
    .bind(&operation.operation_id)
    .execute(&pool)
    .await
    .expect("make retry due");
    let retry = claim_recovery(&pool, &operation.operation_id, "claim:third")
        .await
        .expect("claim retry")
        .expect("retry is due");
    assert_eq!(retry.recovery_attempt_count, 1);

    let mut tx = pool.begin().await.expect("begin stale result");
    let stale = sotto_server::sponsored_billing::record_claimed_checkout(
        &mut tx,
        &operation.operation_id,
        "claim:first",
        "https://checkout.stripe.test/cs_saved",
        "cs_saved",
        Some("cus_saved"),
        Some("sub_saved"),
    )
    .await
    .expect_err("stale provider owner cannot finish the local operation");
    assert!(matches!(stale, SponsoredBillingError::ResultConflict));
    tx.rollback().await.expect("rollback stale result");

    let mut tx = pool.begin().await.expect("begin current result");
    let saved = sotto_server::sponsored_billing::record_claimed_checkout(
        &mut tx,
        &operation.operation_id,
        "claim:third",
        "https://checkout.stripe.test/cs_saved",
        "cs_saved",
        Some("cus_saved"),
        Some("sub_saved"),
    )
    .await
    .expect("current lease records the provider result");
    tx.commit().await.expect("commit current result");
    assert_eq!(saved.state, "checkout_created");
    assert_eq!(
        saved.provider_checkout_session_id.as_deref(),
        Some("cs_saved")
    );
    assert_eq!(saved.recovery_lease_token, None);

    sqlx::query(
        "UPDATE billing_sponsored_operations SET state = 'unknown', recovery_lease_token = $2, \
         recovery_lease_until = now() + interval '5 minutes' WHERE operation_id = $1",
    )
    .bind(&operation.operation_id)
    .bind("claim:expiry")
    .execute(&pool)
    .await
    .expect("model an unresolved checkout with a known session");
    let mut tx = pool
        .begin()
        .await
        .expect("begin expired checkout cancellation");
    sotto_server::founding_allocator::lock_capacity(&mut tx)
        .await
        .expect("lock founding capacity before releasing reservation");
    assert!(!cancel_claimed_rejected_operation(
        &mut tx,
        &operation.operation_id,
        "claim:stale",
        "provider_rejected"
    )
    .await
    .expect("a stale provider owner is a harmless no-op"));
    assert!(
        cancel_claimed_expired_checkout(&mut tx, &operation.operation_id, "claim:expiry")
            .await
            .expect("cancel a provider-confirmed expired checkout")
    );
    tx.commit()
        .await
        .expect("commit expired checkout cancellation");
    let failed_state: String = sqlx::query_scalar(
        "SELECT state FROM billing_sponsored_operations WHERE operation_id = $1",
    )
    .bind(&operation.operation_id)
    .fetch_one(&pool)
    .await
    .expect("read canceled checkout operation");
    assert_eq!(failed_state, "failed");

    sqlx::query("DELETE FROM billing_sponsored_seats WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete seats");
    sqlx::query("DELETE FROM billing_sponsored_operations WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete operations");
    sqlx::query("DELETE FROM organization_memberships WHERE org_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete membership");
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete organisation");
    sqlx::query("DELETE FROM users WHERE id IN ($1, $2)")
        .bind(&owner)
        .bind(&beneficiary)
        .execute(&pool)
        .await
        .expect("delete users");
}
