
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    status,
)

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    PushSubscription,
    PushNotification,
    PushOutboxEvent,
)

from app.push_schemas import (
    PushSubscriptionCreate,
    PushNotificationCreate,
)


router = APIRouter(
    prefix="/push",
    tags=["push notifications"],
)


def serialize_push_notification(notification):
    return {
        "id": notification.id,
        "title": notification.title,
        "body": notification.body,
        "target_url": notification.target_url,
        "status": notification.status,
    }


def matches_push_request(existing, request):
    return (
        existing.title == request.title
        and existing.body == request.body
        and existing.target_url == request.target_url
    )


@router.post(
    "/subscriptions",
    status_code=status.HTTP_201_CREATED,
)
def register_push_subscription(
    subscription: PushSubscriptionCreate,
    db: Session = Depends(get_db),
):
    """
    Register or update a browser push subscription.
    """

    statement = (
        pg_insert(PushSubscription)
        .values(
            endpoint=subscription.endpoint,
            p256dh_key=subscription.keys.p256dh,
            auth_key=subscription.keys.auth,
        )
        .on_conflict_do_update(
            index_elements=[PushSubscription.endpoint],
            set_={
                "p256dh_key": subscription.keys.p256dh,
                "auth_key": subscription.keys.auth,
                "updated_at": func.now(),
                "expired_at": None,
            },
        )
        .returning(PushSubscription.id)
    )

    try:
        subscription_id = db.execute(
            statement
        ).scalar_one()

        db.commit()

    except Exception:
        db.rollback()
        raise

    return {
        "message": "Push subscription registered successfully",
        "subscription_id": subscription_id,
    }


@router.post(
    "/notifications",
    status_code=status.HTTP_202_ACCEPTED,
)
def create_push_notification(
    notification: PushNotificationCreate,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
    ),
    db: Session = Depends(get_db),
):
    """
    Create a push notification and its outbox event atomically.

    The push consumer will later fan out the notification
    to registered, non-expired subscriptions.
    """

    # Step 1: Check whether this idempotency key already exists.
    if idempotency_key is not None:
        existing = (
            db.query(PushNotification)
            .filter(
                PushNotification.idempotency_key == idempotency_key
            )
            .first()
        )

        if existing is not None:
            if not matches_push_request(existing, notification):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Idempotency key already used with "
                        "a different push notification"
                    ),
                )

            return {
                **serialize_push_notification(existing),
                "duplicate": True,
            }

    try:
        # Step 2: Insert the push notification.
        # PostgreSQL handles concurrent idempotency conflicts.
        insert_statement = (
            pg_insert(PushNotification)
            .values(
                title=notification.title,
                body=notification.body,
                target_url=notification.target_url,
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(
                index_elements=[PushNotification.idempotency_key]
            )
            .returning(PushNotification.id)
        )

        push_notification_id = db.execute(
            insert_statement
        ).scalar_one_or_none()

        # Step 3: Resolve a concurrent idempotency conflict.
        if push_notification_id is None:
            existing = (
                db.query(PushNotification)
                .filter(
                    PushNotification.idempotency_key == idempotency_key
                )
                .first()
            )

            if existing is None:
                raise RuntimeError(
                    "Push notification insert conflict "
                    "could not be resolved"
                )

            if not matches_push_request(existing, notification):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Idempotency key already used with "
                        "a different push notification"
                    ),
                )

            db.commit()

            return {
                **serialize_push_notification(existing),
                "duplicate": True,
            }

        # Step 4: Create the outbox event in the same transaction.
        outbox_event = PushOutboxEvent(
            push_notification_id=push_notification_id,
            event_type="push.notification.created",
            payload={
                "push_notification_id": push_notification_id,
            },
        )

        db.add(outbox_event)

        # Step 5: Commit notification and outbox event atomically.
        db.commit()

        return {
            "id": push_notification_id,
            "title": notification.title,
            "body": notification.body,
            "target_url": notification.target_url,
            "status": "queued",
            "duplicate": False,
        }

    except HTTPException:
        # Preserve expected API responses such as HTTP 409.
        raise

    except Exception:
        # Roll back unexpected database or application errors.
        db.rollback()
        raise
