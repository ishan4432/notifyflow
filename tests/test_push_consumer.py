import asyncio

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import sessionmaker

from app.models import (
    PushNotification,
    PushSubscription,
    PushDelivery,
)

from scripts import push_consumer


# ---------------------------------------------------------
# Test helpers
# ---------------------------------------------------------

def bind_test_sessions(monkeypatch, db_session):
    """
    Make consumer database operations use the existing
    test transaction instead of the development database.
    """

    factory = sessionmaker(
        bind=db_session.connection(),
        autoflush=False,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    monkeypatch.setattr(
        push_consumer,
        "SessionLocal",
        factory,
    )


def create_notification(db_session, status="queued"):
    notification = PushNotification(
        title="NotifyFlow Test",
        body="Your notification has arrived",
        target_url="/notifications",
        status=status,
    )

    db_session.add(notification)
    db_session.flush()

    return notification


def create_subscription(
    db_session,
    endpoint_suffix,
    expired=False,
):
    subscription = PushSubscription(
        endpoint=(
            "https://push.example.com/send/"
            + endpoint_suffix
        ),
        p256dh_key="test-public-key",
        auth_key="test-auth-secret",
        expired_at=(
            datetime.now(timezone.utc)
            if expired
            else None
        ),
    )

    db_session.add(subscription)
    db_session.flush()

    return subscription


def create_delivery(
    db_session,
    notification_id,
    subscription_id,
    status="queued",
    attempts=0,
):
    delivery = PushDelivery(
        push_notification_id=notification_id,
        push_subscription_id=subscription_id,
        status=status,
        attempts=attempts,
    )

    db_session.add(delivery)
    db_session.flush()

    return delivery


class FakeMessage:
    """Minimal RabbitMQ message used in unit tests."""

    def __init__(self, message_id="test-push-event-001"):
        self.message_id = message_id
        self.headers = {}
        self.acked = False
        self.nacked = False
        self.processed = False

    async def ack(self):
        self.acked = True
        self.processed = True

    async def nack(self, requeue=True):
        self.nacked = requeue
        self.processed = True


# ---------------------------------------------------------
# Subscription fan-out tests
# ---------------------------------------------------------

def test_prepare_notification_fans_out_to_active_subscriptions(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(db_session)

    active = create_subscription(
        db_session,
        "active",
    )

    create_subscription(
        db_session,
        "expired",
        expired=True,
    )

    result = push_consumer.prepare_push_notification(
        notification.id
    )

    db_session.expire_all()

    deliveries = (
        db_session.query(PushDelivery)
        .filter_by(
            push_notification_id=notification.id,
        )
        .all()
    )

    stored_notification = db_session.get(
        PushNotification,
        notification.id,
    )

    assert result == "ready"
    assert len(deliveries) == 1
    assert deliveries[0].push_subscription_id == active.id
    assert deliveries[0].status == "queued"
    assert stored_notification.status == "processing"


def test_prepare_notification_with_no_active_subscriptions(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(db_session)

    result = push_consumer.prepare_push_notification(
        notification.id
    )

    db_session.expire_all()

    stored_notification = db_session.get(
        PushNotification,
        notification.id,
    )

    assert result == "no_subscribers"
    assert stored_notification.status == "no_subscribers"
    assert stored_notification.completed_at is not None


def test_prepare_notification_does_not_duplicate_deliveries(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(db_session)

    create_subscription(
        db_session,
        "duplicate-test",
    )

    first = push_consumer.prepare_push_notification(
        notification.id
    )

    second = push_consumer.prepare_push_notification(
        notification.id
    )

    db_session.expire_all()

    count = (
        db_session.query(PushDelivery)
        .filter_by(
            push_notification_id=notification.id,
        )
        .count()
    )

    assert first == "ready"
    assert second == "ready"
    assert count == 1


# ---------------------------------------------------------
# Delivery state tests
# ---------------------------------------------------------

def test_claim_delivery_increments_attempts(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "claim-test",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
    )

    claim = push_consumer.claim_delivery(
        delivery.id,
        redelivered=False,
    )

    db_session.expire_all()

    stored_delivery = db_session.get(
        PushDelivery,
        delivery.id,
    )

    assert claim is not None
    assert claim["subscription_id"] == subscription.id
    assert claim["attempts"] == 1

    assert stored_delivery.status == "processing"
    assert stored_delivery.attempts == 1

    assert claim["subscription_info"] == {
        "endpoint": subscription.endpoint,
        "keys": {
            "p256dh": subscription.p256dh_key,
            "auth": subscription.auth_key,
        },
    }


def test_successful_delivery_can_be_finalized(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "accepted-test",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="processing",
        attempts=1,
    )

    push_consumer.save_delivery_result(
        delivery.id,
        result_status="accepted",
        status_code=201,
    )

    final_status = push_consumer.finalize_push_notification(
        notification.id
    )

    db_session.expire_all()

    stored_delivery = db_session.get(
        PushDelivery,
        delivery.id,
    )

    stored_notification = db_session.get(
        PushNotification,
        notification.id,
    )

    assert final_status == "accepted"
    assert stored_delivery.status == "accepted"
    assert stored_delivery.push_status_code == 201
    assert stored_delivery.accepted_at is not None
    assert stored_notification.status == "accepted"
    assert stored_notification.completed_at is not None


def test_expired_subscription_is_deactivated(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "expired-during-send",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="processing",
        attempts=1,
    )

    push_consumer.save_delivery_result(
        delivery.id,
        result_status="expired",
        last_error="HTTP 410",
        expire_subscription=True,
    )

    final_status = push_consumer.finalize_push_notification(
        notification.id
    )

    db_session.expire_all()

    stored_subscription = db_session.get(
        PushSubscription,
        subscription.id,
    )

    stored_delivery = db_session.get(
        PushDelivery,
        delivery.id,
    )

    assert stored_subscription.expired_at is not None
    assert stored_delivery.status == "expired"
    assert final_status == "failed"


def test_partial_failure_is_recorded_correctly(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    accepted_subscription = create_subscription(
        db_session,
        "partial-accepted",
    )

    failed_subscription = create_subscription(
        db_session,
        "partial-failed",
    )

    accepted_delivery = create_delivery(
        db_session,
        notification.id,
        accepted_subscription.id,
        status="accepted",
        attempts=1,
    )

    failed_delivery = create_delivery(
        db_session,
        notification.id,
        failed_subscription.id,
        status="failed",
        attempts=3,
    )

    result = push_consumer.finalize_push_notification(
        notification.id
    )

    db_session.expire_all()

    stored_notification = db_session.get(
        PushNotification,
        notification.id,
    )

    assert accepted_delivery.id != failed_delivery.id
    assert result == "partial_failure"
    assert stored_notification.status == "partial_failure"


# ---------------------------------------------------------
# Crash recovery and retry eligibility tests
# ---------------------------------------------------------

def test_processing_delivery_is_reclaimed_after_redelivery(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "recovery-test",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="processing",
        attempts=1,
    )

    delivery_ids = push_consumer.get_delivery_ids(
        notification.id,
        redelivered=True,
    )

    claim = push_consumer.claim_delivery(
        delivery.id,
        redelivered=True,
    )

    db_session.expire_all()

    stored = db_session.get(
        PushDelivery,
        delivery.id,
    )

    assert delivery.id in delivery_ids
    assert claim is not None
    assert claim["attempts"] == 2
    assert stored.status == "processing"
    assert stored.attempts == 2


def test_processing_delivery_is_not_stolen_without_redelivery(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "concurrency-test",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="processing",
        attempts=1,
    )

    delivery_ids = push_consumer.get_delivery_ids(
        notification.id,
        redelivered=False,
    )

    claim = push_consumer.claim_delivery(
        delivery.id,
        redelivered=False,
    )

    assert delivery.id not in delivery_ids
    assert claim is None


def test_retrying_delivery_is_eligible_without_redelivery(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "retrying-test",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="retrying",
        attempts=1,
    )

    delivery_ids = push_consumer.get_delivery_ids(
        notification.id,
        redelivered=False,
    )

    claim = push_consumer.claim_delivery(
        delivery.id,
        redelivered=False,
    )

    assert delivery.id in delivery_ids
    assert claim is not None
    assert claim["attempts"] == 2


# ---------------------------------------------------------
# RabbitMQ acknowledgement tests
# ---------------------------------------------------------

def test_forward_and_ack_acknowledges_after_publish(
    monkeypatch,
):
    calls = []

    async def fake_publish_copy(
        message,
        publisher_channel,
        destination,
        extra_headers,
    ):
        calls.append((destination, extra_headers))

    monkeypatch.setattr(
        push_consumer,
        "publish_copy",
        fake_publish_copy,
    )

    message = FakeMessage()

    result = asyncio.run(
        push_consumer.forward_and_ack(
            message,
            object(),
            push_consumer.RETRY_QUEUE,
            {"x-retry-count": 1},
        )
    )

    assert result is True
    assert message.acked is True
    assert message.nacked is False

    assert calls == [
        (
            push_consumer.RETRY_QUEUE,
            {"x-retry-count": 1},
        )
    ]


def test_forward_and_ack_requeues_when_publish_fails(
    monkeypatch,
):
    async def failing_publish_copy(*args, **kwargs):
        raise RuntimeError("Simulated RabbitMQ failure")

    monkeypatch.setattr(
        push_consumer,
        "publish_copy",
        failing_publish_copy,
    )

    message = FakeMessage()

    result = asyncio.run(
        push_consumer.forward_and_ack(
            message,
            object(),
            push_consumer.RETRY_QUEUE,
            {},
        )
    )

    assert result is False
    assert message.acked is False
    assert message.nacked is True


# ---------------------------------------------------------
# Retry scheduling and DLQ tests
# ---------------------------------------------------------

def test_schedule_retry_publishes_retry_count(
    monkeypatch,
):
    calls = []

    async def fake_forward_and_ack(
        message,
        publisher_channel,
        destination,
        extra_headers,
    ):
        calls.append((destination, extra_headers))
        return True

    monkeypatch.setattr(
        push_consumer,
        "forward_and_ack",
        fake_forward_and_ack,
    )

    message = FakeMessage(
        message_id="test-push-retry-001",
    )

    asyncio.run(
        push_consumer.schedule_retry(
            message,
            object(),
            retry_count=0,
            reason="Temporary provider failure",
            push_notification_id=123,
        )
    )

    assert calls == [
        (
            push_consumer.RETRY_QUEUE,
            {"x-retry-count": 1},
        )
    ]


def test_retry_exhaustion_moves_event_to_dlq(
    monkeypatch,
):
    calls = []

    monkeypatch.setattr(
        push_consumer,
        "mark_pending_deliveries_failed",
        lambda notification_id, reason: calls.append(
            ("failed", notification_id, reason)
        ),
    )

    def fake_finalize(notification_id):
        calls.append(("finalized", notification_id))
        return "failed"

    monkeypatch.setattr(
        push_consumer,
        "finalize_push_notification",
        fake_finalize,
    )

    async def fake_send_to_dlq(
        message,
        publisher_channel,
        reason,
    ):
        calls.append(("dlq", reason))

    monkeypatch.setattr(
        push_consumer,
        "send_to_dlq",
        fake_send_to_dlq,
    )

    message = FakeMessage(
        message_id="test-push-dlq-001",
    )

    asyncio.run(
        push_consumer.schedule_retry(
            message,
            object(),
            retry_count=2,
            reason="Maximum retries reached",
            push_notification_id=123,
        )
    )

    assert calls[0] == (
        "failed",
        123,
        "Maximum retries reached",
    )

    assert calls[1] == ("finalized", 123)

    assert calls[2][0] == "dlq"

    assert (
        "Maximum push processing attempts reached: 3"
        in calls[2][1]
    )

def test_stale_processing_delivery_is_reclaimed_after_application_retry(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    notification = create_notification(
        db_session,
        status="processing",
    )

    subscription = create_subscription(
        db_session,
        "application-retry-recovery",
    )

    delivery = create_delivery(
        db_session,
        notification.id,
        subscription.id,
        status="processing",
        attempts=1,
    )

    delivery.updated_at = (
        datetime.now(timezone.utc)
        - timedelta(
            seconds=push_consumer.PUSH_DELIVERY_LEASE_SECONDS + 10
        )
    )

    db_session.flush()

    delivery_ids = push_consumer.get_delivery_ids(
        notification.id,
        redelivered=False,
    )

    claim = push_consumer.claim_delivery(
        delivery.id,
        redelivered=False,
    )

    db_session.expire_all()

    stored = db_session.get(
        PushDelivery,
        delivery.id,
    )

    assert delivery.id in delivery_ids
    assert claim is not None
    assert claim["attempts"] == 2
    assert stored.status == "processing"
    assert stored.attempts == 2


# ---------------------------------------------------------
# Prometheus worker metrics tests
# ---------------------------------------------------------

def get_worker_metric_value(sample_name, labels=None):
    """Read a metric sample from the default Prometheus registry."""

    from prometheus_client import generate_latest
    from prometheus_client.parser import text_string_to_metric_families

    expected_labels = labels or {}
    metrics_text = generate_latest().decode("utf-8")

    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            if (
                sample.name == sample_name
                and sample.labels == expected_labels
            ):
                return float(sample.value)

    return 0.0


def test_delivery_result_metrics_are_recorded_after_persistence(
    monkeypatch,
    db_session,
):
    bind_test_sessions(monkeypatch, db_session)

    statuses = ["accepted", "expired", "retrying", "failed"]

    for index, result_status in enumerate(statuses):
        notification = create_notification(
            db_session,
            status="processing",
        )

        subscription = create_subscription(
            db_session,
            f"metrics-result-{index}",
        )

        delivery = create_delivery(
            db_session,
            notification.id,
            subscription.id,
            status="processing",
            attempts=1,
        )

        labels = {"status": result_status}

        before = get_worker_metric_value(
            "notifyflow_push_delivery_results_total",
            labels,
        )

        push_consumer.save_delivery_result(
            delivery.id,
            result_status=result_status,
            last_error=(
                "Simulated test failure"
                if result_status != "accepted"
                else None
            ),
            status_code=(
                201 if result_status == "accepted" else None
            ),
            expire_subscription=(result_status == "expired"),
        )

        after = get_worker_metric_value(
            "notifyflow_push_delivery_results_total",
            labels,
        )

        assert after == before + 1


def test_provider_attempt_metric_increments_before_provider_call(
    monkeypatch,
):
    from types import SimpleNamespace

    delivery_ids = [101]

    claim = {
        "subscription_info": {
            "endpoint": "https://push.example.com/test",
            "keys": {
                "p256dh": "test-key",
                "auth": "test-auth",
            },
        },
        "attempts": 1,
    }

    monkeypatch.setattr(
        push_consumer,
        "get_delivery_ids",
        lambda notification_id, redelivered: delivery_ids,
    )

    monkeypatch.setattr(
        push_consumer,
        "claim_delivery",
        lambda delivery_id, redelivered: claim,
    )

    saved_results = []

    monkeypatch.setattr(
        push_consumer,
        "save_delivery_result",
        lambda delivery_id, **kwargs: saved_results.append(
            (delivery_id, kwargs)
        ),
    )

    before = get_worker_metric_value(
        "notifyflow_push_provider_attempts_total"
    )

    class FakeProvider:
        async def send(self, subscription_info, payload):
            current = get_worker_metric_value(
                "notifyflow_push_provider_attempts_total"
            )

            # The attempt must be counted before provider.send().
            assert current == before + 1

            return SimpleNamespace(status_code=201)

    asyncio.run(
        push_consumer.deliver_pending(
            push_notification_id=100,
            title="Metrics Test",
            body="Testing provider metrics",
            target_url="/",
            provider=FakeProvider(),
            redelivered=False,
        )
    )

    after = get_worker_metric_value(
        "notifyflow_push_provider_attempts_total"
    )

    assert after == before + 1

    assert saved_results == [
        (
            101,
            {
                "result_status": "accepted",
                "status_code": 201,
            },
        )
    ]


def test_recovery_metric_increments_only_after_confirmed_forwarding(
    monkeypatch,
):
    outcomes = iter([True, False])

    async def fake_forward_and_ack(*args, **kwargs):
        return next(outcomes)

    monkeypatch.setattr(
        push_consumer,
        "forward_and_ack",
        fake_forward_and_ack,
    )

    before = get_worker_metric_value(
        "notifyflow_push_recovery_scheduled_total"
    )

    first_result = asyncio.run(
        push_consumer.schedule_recovery(
            FakeMessage("recovery-success"),
            object(),
            "Test recovery",
        )
    )

    assert first_result is True

    after_success = get_worker_metric_value(
        "notifyflow_push_recovery_scheduled_total"
    )

    assert after_success == before + 1

    second_result = asyncio.run(
        push_consumer.schedule_recovery(
            FakeMessage("recovery-failure"),
            object(),
            "Test forwarding failure",
        )
    )

    assert second_result is False

    after_failure = get_worker_metric_value(
        "notifyflow_push_recovery_scheduled_total"
    )

    assert after_failure == after_success


def test_dlq_metric_increments_only_after_confirmed_forwarding(
    monkeypatch,
):
    outcomes = iter([True, False])

    async def fake_forward_and_ack(*args, **kwargs):
        return next(outcomes)

    monkeypatch.setattr(
        push_consumer,
        "forward_and_ack",
        fake_forward_and_ack,
    )

    before = get_worker_metric_value(
        "notifyflow_push_dlq_events_total"
    )

    # Successful forwarding.
    asyncio.run(
        push_consumer.send_to_dlq(
            FakeMessage("dlq-success"),
            object(),
            "Test failure",
        )
    )

    after_success = get_worker_metric_value(
        "notifyflow_push_dlq_events_total"
    )

    assert after_success == before + 1

    # Failed forwarding must not increment the counter.
    asyncio.run(
        push_consumer.send_to_dlq(
            FakeMessage("dlq-failure"),
            object(),
            "Simulated RabbitMQ failure",
        )
    )

    after_failure = get_worker_metric_value(
        "notifyflow_push_dlq_events_total"
    )

    assert after_failure == after_success


# ---------------------------------------------------------
# Unexpected consumer exception tests
# ---------------------------------------------------------

def test_unhandled_consumer_exception_increments_metric_and_requeues(
    monkeypatch,
):
    message = FakeMessage()
    calls = {"metric": 0, "duration": 0}

    async def failing_process_message(
        message,
        publisher_channel,
        provider,
    ):
        raise RuntimeError("Simulated unexpected consumer error")

    def fake_increment():
        calls["metric"] += 1

    def fake_observe(value):
        calls["duration"] += 1

    monkeypatch.setattr(
        push_consumer,
        "process_message",
        failing_process_message,
    )

    monkeypatch.setattr(
        push_consumer.PUSH_CONSUMER_ERRORS,
        "inc",
        fake_increment,
    )

    monkeypatch.setattr(
        push_consumer.PUSH_MESSAGE_PROCESSING_DURATION,
        "observe",
        fake_observe,
    )

    asyncio.run(
        push_consumer.handle_push_message(
            message,
            publisher_channel=object(),
            provider=object(),
        )
    )

    assert calls["metric"] == 1
    assert calls["duration"] == 1
    assert message.nacked is True
    assert message.acked is False
    assert message.processed is True
