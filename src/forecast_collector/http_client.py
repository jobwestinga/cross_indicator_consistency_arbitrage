from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import Settings
from .endpoints import ForecastTraderEndpoints
from .models import ApiResponseEnvelope, RequestSpec


logger = logging.getLogger(__name__)


class ForecastTraderClient:
    def __init__(self, settings: Settings, session: httpx.Client | None = None) -> None:
        self.settings = settings
        self.endpoints = ForecastTraderEndpoints(
            public_prefix=settings.ibkr_public_prefix,
            exchange=settings.ibkr_exchange,
        )
        self._client = session or httpx.Client(
            base_url=settings.ibkr_base_url,
            timeout=settings.http_timeout_seconds,
            headers={
                "Accept": "application/json",
                "User-Agent": "forecasttrader-collector/0.1.0",
            },
        )
        self._minimum_interval = (
            1.0 / settings.http_requests_per_second if settings.http_requests_per_second > 0 else 0.0
        )
        self._last_request_started_at = 0.0
        max_wait_seconds = max(
            settings.http_retry_backoff_seconds,
            settings.http_retry_backoff_seconds * max(settings.http_max_retries, 1),
        )
        self._retrying = Retrying(
            reraise=True,
            stop=stop_after_attempt(settings.http_max_retries),
            wait=wait_exponential(
                multiplier=settings.http_retry_backoff_seconds,
                min=settings.http_retry_backoff_seconds,
                max=max_wait_seconds,
            ),
            retry=retry_if_exception_type((httpx.HTTPError, ValueError)),
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ForecastTraderClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def get_market(self, underlying_conid: int) -> ApiResponseEnvelope:
        return self._request(self.endpoints.market(underlying_conid))

    def get_contract_details(self, conid: int) -> ApiResponseEnvelope:
        return self._request(self.endpoints.contract_details(conid))

    def get_history(self, conid: int, period: str) -> ApiResponseEnvelope:
        return self._request(self.endpoints.history(conid, period))

    def get_open_interest(self, conid: int) -> ApiResponseEnvelope:
        return self._request(self.endpoints.open_interest(conid))

    def get_projected_probabilities(self, underlying_conid: int) -> ApiResponseEnvelope:
        return self._request(self.endpoints.projected_probabilities(underlying_conid))

    def _respect_rate_limit(self) -> None:
        if self._minimum_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_started_at
        remaining = self._minimum_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_started_at = time.monotonic()

    def _do_request(self, spec: RequestSpec) -> httpx.Response:
        for attempt in self._retrying:
            with attempt:
                self._respect_rate_limit()
                logger.debug("requesting %s", spec.path)
                response = self._client.get(spec.path, params=spec.params)
                response.raise_for_status()
                return response
        raise RuntimeError("Retry loop exited unexpectedly")

    def _request(self, spec: RequestSpec) -> ApiResponseEnvelope:
        response = self._do_request(spec)
        payload = response.json()
        return ApiResponseEnvelope(
            endpoint_name=spec.endpoint_name,
            request_url=str(response.request.url),
            query_params=dict(spec.params),
            http_status=response.status_code,
            response_json=payload,
            fetched_at=datetime.now(tz=UTC),
        )
