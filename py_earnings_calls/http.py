from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

import requests

from py_earnings_calls.config import AppConfig
from py_earnings_calls.rate_limit import SharedRateLimiter


TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class HttpFailure:
    url: str
    reason: str
    status_code: int | None = None
    error: str | None = None
    error_class: str | None = None


class HttpRequestError(RuntimeError):
    def __init__(self, failure: HttpFailure, *, attempts: int, max_attempts: int) -> None:
        self.failure = failure
        self.attempts = attempts
        self.max_attempts = max_attempts
        super().__init__(f"request failed for {failure.url}: {failure.reason}")


class HttpClient:
    def __init__(self, config: AppConfig, *, limiter: SharedRateLimiter | None = None) -> None:
        self._config = config
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": config.user_agent})
        self._limiter = limiter or SharedRateLimiter(config.max_requests_per_second)

    def request_text(self, url: str, *, params: dict[str, Any] | None = None, max_attempts: int = 3) -> str:
        response = self._request("GET", url, params=params, max_attempts=max_attempts)
        return response.text

    def request_json(self, url: str, *, params: dict[str, Any] | None = None, max_attempts: int = 3) -> Any:
        response = self._request("GET", url, params=params, max_attempts=max_attempts)
        return response.json()

    def _request(self, method: str, url: str, *, params: dict[str, Any] | None, max_attempts: int) -> requests.Response:
        last_failure: HttpFailure | None = None
        for attempt in range(1, max_attempts + 1):
            self._limiter.wait()
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    timeout=(self._config.request_timeout_connect, self._config.request_timeout_read),
                )
                if response.status_code >= 400:
                    reason = "http_error"
                    last_failure = HttpFailure(url=url, reason=reason, status_code=response.status_code)
                    if response.status_code in TRANSIENT_STATUS_CODES and attempt < max_attempts:
                        time.sleep(float(attempt))
                        continue
                    raise HttpRequestError(last_failure, attempts=attempt, max_attempts=max_attempts)
                return response
            except requests.RequestException as exc:
                last_failure = HttpFailure(
                    url=url,
                    reason="request_exception",
                    error=str(exc),
                    error_class=type(exc).__name__,
                )
                if attempt < max_attempts:
                    time.sleep(float(attempt))
                    continue
                raise HttpRequestError(last_failure, attempts=attempt, max_attempts=max_attempts) from exc
        if last_failure is None:
            last_failure = HttpFailure(url=url, reason="unknown_http_failure")
        raise HttpRequestError(last_failure, attempts=max_attempts, max_attempts=max_attempts)
