"""A narrow, testable client for the ONS filter-output workflow."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

from pipeline_quality.exceptions import FilterOutputTimeout, OnsApiError

BASE_URL = "https://api.beta.ons.gov.uk/v1"


class OnsClient:
    """ONS client with explicit retry and polling boundaries."""

    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        user_agent: str = "data-pipeline-quality-lab/0.1.0",
        max_retries: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=BASE_URL,
            timeout=httpx.Timeout(30.0),
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        self._max_retries = max_retries
        self._sleep = sleeper

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> OnsClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                if attempt == self._max_retries:
                    raise OnsApiError(f"ONS request failed after {attempt + 1} attempts") from exc
                self._sleep(2**attempt)
                continue

            retryable = response.status_code == 429 or response.status_code >= 500
            if retryable and attempt < self._max_retries:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else float(2**attempt)
                self._sleep(delay)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise OnsApiError(
                    f"ONS returned HTTP {response.status_code} for {method} {path}"
                ) from exc
            return response

        raise AssertionError("retry loop exhausted unexpectedly")

    def get_dataset_version(self, dataset_id: str, edition: str, version: int) -> dict[str, Any]:
        response = self._request(
            "GET", f"/datasets/{dataset_id}/editions/{edition}/versions/{version}"
        )
        return self._json(response)

    def create_filter(self, definition: dict[str, Any]) -> dict[str, Any]:
        return self._json(self._request("POST", "/filters", json=definition))

    def submit_filter(self, filter_id: str) -> dict[str, Any]:
        return self._json(self._request("POST", f"/filters/{filter_id}/submit"))

    def get_filter_output(self, output_id: str) -> dict[str, Any]:
        return self._json(self._request("GET", f"/filter-outputs/{output_id}"))

    def wait_for_downloads(
        self,
        output_id: str,
        *,
        max_attempts: int = 10,
        poll_interval: float = 1.0,
    ) -> dict[str, Any]:
        for attempt in range(max_attempts):
            output = self.get_filter_output(output_id)
            if self._has_ready_download(output, "csv") and self._has_ready_download(output, "csvw"):
                return output
            if any(event.get("type") == "error" for event in output.get("events", [])):
                raise OnsApiError(f"ONS filter output {output_id} reported an error")
            if attempt + 1 < max_attempts:
                self._sleep(poll_interval)
        raise FilterOutputTimeout(
            f"ONS filter output {output_id} was not ready after {max_attempts} attempts"
        )

    def download(self, url: str) -> httpx.Response:
        response = self._request("GET", url, headers={"Accept": "*/*"})
        if not response.content:
            raise OnsApiError(f"ONS artifact at {url} was empty")
        return response

    @staticmethod
    def _has_ready_download(output: dict[str, Any], kind: str) -> bool:
        item = output.get("downloads", {}).get(kind, {})
        return bool((item.get("public") or item.get("href")) and not item.get("skipped", False))

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        content_type = response.headers.get("Content-Type", "")
        if "json" not in content_type.lower():
            raise OnsApiError(
                f"Expected JSON from {response.request.url}, received {content_type or 'unknown'}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise OnsApiError(f"Malformed JSON from {response.request.url}") from exc
        if not isinstance(payload, dict):
            raise OnsApiError(f"Expected a JSON object from {response.request.url}")
        return payload
