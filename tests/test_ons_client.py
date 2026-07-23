from __future__ import annotations

from collections import deque

import httpx
import pytest

from pipeline_quality.exceptions import FilterOutputTimeout, OnsApiError
from pipeline_quality.ons_client import OnsClient


def response(status: int, payload: object, *, headers: dict[str, str] | None = None):
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=payload, headers=headers, request=request)

    return _handler


def scripted_client(*handlers):
    queue = deque(handlers)

    def _handler(request: httpx.Request) -> httpx.Response:
        if not queue:
            raise AssertionError("unexpected HTTP request")
        return queue.popleft()(request)

    return httpx.Client(
        base_url="https://api.beta.ons.gov.uk/v1",
        transport=httpx.MockTransport(_handler),
    )


def test_create_and_submit_filter(load_fixture) -> None:
    """The client must carry the created filter identity into submission."""
    client = scripted_client(
        response(201, load_fixture("filter-created.json")),
        response(202, load_fixture("filter-submitted.json")),
    )
    ons = OnsClient(client=client)

    created = ons.create_filter(load_fixture("filter-definition.json"))
    submitted = ons.submit_filter(created["filter_id"])

    assert submitted["filter_output_id"] == "output-123"


def test_polling_waits_for_csv_and_csvw(load_fixture) -> None:
    """A filter is ready only when both data and schema metadata are downloadable."""
    sleeps: list[float] = []
    client = scripted_client(
        response(200, load_fixture("filter-pending.json")),
        response(200, load_fixture("filter-ready.json")),
    )
    ons = OnsClient(client=client, sleeper=sleeps.append)

    output = ons.wait_for_downloads("output-123", max_attempts=2, poll_interval=0.25)

    assert output["published"] is True
    assert sleeps == [0.25]


def test_polling_times_out(load_fixture) -> None:
    """A permanently pending upstream job must stop at a finite boundary."""
    client = scripted_client(
        response(200, load_fixture("filter-pending.json")),
        response(200, load_fixture("filter-pending.json")),
    )
    ons = OnsClient(client=client, sleeper=lambda _: None)

    with pytest.raises(FilterOutputTimeout, match="not ready"):
        ons.wait_for_downloads("output-123", max_attempts=2)


def test_polling_stops_on_source_error(load_fixture) -> None:
    """An explicit provider error is terminal and must remain visible."""
    client = scripted_client(response(200, load_fixture("filter-error.json")))
    ons = OnsClient(client=client)

    with pytest.raises(OnsApiError, match="reported an error"):
        ons.wait_for_downloads("output-123")


def test_rate_limit_honours_retry_after() -> None:
    """HTTP 429 handling must respect the provider's requested delay."""
    sleeps: list[float] = []
    client = scripted_client(
        response(429, {"error": "slow down"}, headers={"Retry-After": "3"}),
        response(200, {"id": "TS009"}, headers={"Content-Type": "application/json"}),
    )
    ons = OnsClient(client=client, sleeper=sleeps.append)

    result = ons.get_dataset_version("TS009", "2021", 1)

    assert result == {"id": "TS009"}
    assert sleeps == [3.0]


def test_html_error_page_is_not_treated_as_json() -> None:
    """A proxy error page must not be mistaken for a valid API object."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html>maintenance</html>",
            headers={"Content-Type": "text/html"},
            request=request,
        )

    ons = OnsClient(
        client=httpx.Client(
            base_url="https://api.beta.ons.gov.uk/v1",
            transport=httpx.MockTransport(handler),
        )
    )

    with pytest.raises(OnsApiError, match="Expected JSON"):
        ons.get_dataset_version("TS009", "2021", 1)


def test_transport_failure_is_bounded() -> None:
    """Repeated network errors must not create an infinite retry loop."""
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("offline", request=request)

    ons = OnsClient(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        max_retries=2,
        sleeper=lambda _: None,
    )

    with pytest.raises(OnsApiError, match="after 3 attempts"):
        ons.get_dataset_version("TS009", "2021", 1)
    assert attempts == 3


def test_non_retryable_http_error_is_descriptive() -> None:
    """Permanent HTTP errors should preserve useful request context."""
    client = scripted_client(response(404, {"error": "missing"}))
    ons = OnsClient(client=client)

    with pytest.raises(OnsApiError, match="HTTP 404"):
        ons.get_dataset_version("missing", "2021", 1)


def test_download_rejects_empty_artifact() -> None:
    """An empty download cannot become a successful ingestion input."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", request=request)

    ons = OnsClient(
        client=httpx.Client(
            base_url="https://api.beta.ons.gov.uk/v1",
            transport=httpx.MockTransport(handler),
        )
    )

    with pytest.raises(OnsApiError, match="was empty"):
        ons.download("/empty.csv")
