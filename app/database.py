from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Settings(BaseSettings):
    # PostgreSQL
    DATABASE_URL: str

    # SMS provider
    SMS_PROVIDER: str = "mock"

    # MSG91
    MSG91_AUTH_KEY: SecretStr | None = None
    MSG91_TEMPLATE_ID: str | None = None
    MSG91_MESSAGE_VARIABLE: str = "VAR1"
    MSG91_API_URL: str = "https://control.msg91.com/api/v5/flow"
    MSG91_TIMEOUT_SECONDS: float = 10.0

    # Web Push / VAPID
    VAPID_PRIVATE_KEY_PATH: str | None = None
    VAPID_PUBLIC_KEY: str | None = None
    VAPID_SUBJECT: str | None = None

    WEB_PUSH_TTL_SECONDS: int = 3600
    WEB_PUSH_TIMEOUT_SECONDS: float = 10.0

    # DLR webhook security
    DLR_WEBHOOK_SECRET: SecretStr
    DLR_WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS: int = 300

    # RabbitMQ
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USERNAME: str = "notifyflow_app"
    RABBITMQ_PASSWORD: SecretStr
    RABBITMQ_VHOST: str = "notifyflow"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()


engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
)


SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()
