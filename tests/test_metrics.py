from prometheus_client.parser import text_string_to_metric_families


def get_metric_value(metrics_text, sample_name, labels):
    """Find a Prometheus sample by its name and labels."""

    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            if sample.name == sample_name and sample.labels == labels:
                return sample.value

    return 0.0


def test_metrics_endpoint_returns_prometheus_format(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(
        "text/plain; version=1.0.0"
    )

    assert "notifyflow_api_requests_total" in response.text
    assert "notifyflow_api_responses_total" in response.text
    assert "notifyflow_api_request_duration_seconds" in response.text

    # Scraping /metrics must not count as an API request.
    assert 'endpoint="/metrics"' not in response.text


def test_health_request_increments_request_counter(client):
    before = client.get("/metrics").text

    labels = {
        "method": "GET",
        "endpoint": "/health",
    }

    previous_count = get_metric_value(
        before,
        "notifyflow_api_requests_total",
        labels,
    )

    response = client.get("/health")

    assert response.status_code == 200

    after = client.get("/metrics").text

    current_count = get_metric_value(
        after,
        "notifyflow_api_requests_total",
        labels,
    )

    assert current_count == previous_count + 1


def test_api_response_status_is_recorded(client):
    before = client.get("/metrics").text

    labels = {
        "method": "GET",
        "endpoint": "/notifications/{notification_id}",
        "status_code": "404",
    }

    previous_count = get_metric_value(
        before,
        "notifyflow_api_responses_total",
        labels,
    )

    response = client.get("/notifications/99999999")

    assert response.status_code == 404

    after = client.get("/metrics").text

    current_count = get_metric_value(
        after,
        "notifyflow_api_responses_total",
        labels,
    )

    assert current_count == previous_count + 1


def test_request_latency_histogram_is_recorded(client):
    before = client.get("/metrics").text

    labels = {
        "method": "GET",
        "endpoint": "/health",
    }

    previous_count = get_metric_value(
        before,
        "notifyflow_api_request_duration_seconds_count",
        labels,
    )

    response = client.get("/health")

    assert response.status_code == 200

    after = client.get("/metrics").text

    current_count = get_metric_value(
        after,
        "notifyflow_api_request_duration_seconds_count",
        labels,
    )

    assert current_count == previous_count + 1
