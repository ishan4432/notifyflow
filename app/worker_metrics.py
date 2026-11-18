from prometheus_client import Counter, Histogram


# ---------------------------------------------------------
# Push consumer metrics
# ---------------------------------------------------------

PUSH_PROVIDER_ATTEMPTS = Counter(
    "notifyflow_push_provider_attempts",
    "Total number of attempts to send push notifications to the provider.",
)

PUSH_DELIVERY_RESULTS = Counter(
    "notifyflow_push_delivery_results",
    "Total number of persisted push delivery outcomes.",
    labelnames=("status",),
)

PUSH_RECOVERY_SCHEDULED = Counter(
    "notifyflow_push_recovery_scheduled",
    "Total number of recovery events successfully scheduled.",
)

PUSH_DLQ_EVENTS = Counter(
    "notifyflow_push_dlq_events",
    "Total number of events successfully forwarded to the push DLQ.",
)

PUSH_MESSAGE_PROCESSING_DURATION = Histogram(
    "notifyflow_push_message_processing_duration_seconds",
    "Time spent processing a push message.",
    buckets=(
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
    ),
)

PUSH_CONSUMER_ERRORS = Counter(
    "notifyflow_push_consumer_errors",
    "Number of unhandled errors in the push consumer callback.",
)


# ---------------------------------------------------------
# Push outbox publisher metrics
# ---------------------------------------------------------

PUSH_OUTBOX_PUBLISH_ATTEMPTS = Counter(
    "notifyflow_push_outbox_publish_attempts",
    "Number of push outbox events submitted to RabbitMQ.",
)

PUSH_OUTBOX_PUBLISH_CONFIRMED = Counter(
    "notifyflow_push_outbox_publish_confirmed",
    "Number of push outbox events confirmed by RabbitMQ.",
)

PUSH_OUTBOX_PUBLISH_FAILURES = Counter(
    "notifyflow_push_outbox_publish_failures",
    "Number of failed push outbox publishing operations.",
)

PUSH_OUTBOX_PUBLISH_DURATION = Histogram(
    "notifyflow_push_outbox_publish_duration_seconds",
    "Time spent publishing push outbox events to RabbitMQ.",
    buckets=(
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    ),
)


# SMS outbox publisher metrics
SMS_OUTBOX_PUBLISH_ATTEMPTS = Counter(
    "notifyflow_sms_outbox_publish_attempts",
    "Number of SMS outbox events submitted for RabbitMQ publishing",
)

SMS_OUTBOX_PUBLISH_CONFIRMED = Counter(
    "notifyflow_sms_outbox_publish_confirmed",
    "Number of SMS outbox events confirmed by RabbitMQ",
)

SMS_OUTBOX_PUBLISH_FAILURES = Counter(
    "notifyflow_sms_outbox_publish_failures",
    "Number of SMS outbox publishing failures",
)

SMS_OUTBOX_PUBLISH_DURATION = Histogram(
    "notifyflow_sms_outbox_publish_duration_seconds",
    "Time spent publishing SMS events to RabbitMQ",
    buckets=(
        0.005, 0.01, 0.025, 0.05, 0.1,
        0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ),
)


# SMS consumer metrics
SMS_PROVIDER_ATTEMPTS = Counter(
    "notifyflow_sms_provider_attempts",
    "Number of SMS provider submission attempts",
)

SMS_DELIVERY_RESULTS = Counter(
    "notifyflow_sms_delivery_results",
    "SMS processing outcomes: sent, failed, or retrying",
    labelnames=("status",),
)

SMS_DLQ_EVENTS = Counter(
    "notifyflow_sms_dlq_events",
    "Number of SMS events successfully forwarded to the dead-letter queue",
)

SMS_MESSAGE_PROCESSING_DURATION = Histogram(
    "notifyflow_sms_message_processing_duration_seconds",
    "Time spent processing an SMS RabbitMQ message",
    buckets=(
        0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
        1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
    ),
)

SMS_CONSUMER_ERRORS = Counter(
    "notifyflow_sms_consumer_errors",
    "Unexpected exceptions escaping SMS message processing",
)
