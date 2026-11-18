from time import perf_counter

from prometheus_client import Counter, Histogram


# Total HTTP requests, grouped by method and FastAPI route.
API_REQUESTS = Counter(
    "notifyflow_api_requests",
    "Total number of HTTP requests received by NotifyFlow.",
    labelnames=("method", "endpoint"),
)

# HTTP responses grouped by method, endpoint and status code.
API_RESPONSES = Counter(
    "notifyflow_api_responses",
    "Total number of HTTP responses returned by NotifyFlow.",
    labelnames=("method", "endpoint", "status_code"),
)

# Request duration in seconds.
API_REQUEST_DURATION = Histogram(
    "notifyflow_api_request_duration_seconds",
    "HTTP request processing duration in seconds.",
    labelnames=("method", "endpoint"),
    buckets=(
        0.0005,  # 0.5 ms
        0.001,   # 1 ms
        0.0015,  # 1.5 ms
        0.002,   # 2 ms
        0.003,   # 3 ms
        0.005,   # 5 ms
        0.0075,  # 7.5 ms
        0.01,    # 10 ms
        0.025,   # 25 ms
        0.05,    # 50 ms
        0.1,     # 100 ms
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    ),
)


class PrometheusMiddleware:
    """
    ASGI middleware that records HTTP metrics.

    Route templates are used instead of raw URLs to prevent
    high-cardinality labels such as individual notification IDs.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        # Only instrument HTTP requests.
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Avoid counting Prometheus scraping its own endpoint.
        if scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return

        start_time = perf_counter()
        status_code = "500"

        async def send_wrapper(message):
            nonlocal status_code

            if message["type"] == "http.response.start":
                status_code = str(message["status"])

            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = perf_counter() - start_time

            route = scope.get("route")
            endpoint = getattr(route, "path", "unmatched")
            method = scope.get("method", "UNKNOWN")

            API_REQUESTS.labels(
                method=method,
                endpoint=endpoint,
            ).inc()

            API_RESPONSES.labels(
                method=method,
                endpoint=endpoint,
                status_code=status_code,
            ).inc()

            API_REQUEST_DURATION.labels(
                method=method,
                endpoint=endpoint,
            ).observe(elapsed)
