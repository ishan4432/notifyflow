FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --gid 10001 notifyflow \
    && useradd --uid 10001 --gid notifyflow \
       --create-home --shell /usr/sbin/nologin notifyflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=notifyflow:notifyflow app ./app
COPY --chown=notifyflow:notifyflow scripts ./scripts
COPY --chown=notifyflow:notifyflow migrations ./migrations
COPY --chown=notifyflow:notifyflow alembic.ini .

RUN chmod -R a+rX /app/app /app/scripts /app/migrations

USER notifyflow

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
