"""
Async MLB Stats API client.

Features:
- Token-bucket rate limiter (default 8 req/s, leaving 20% below observed 10 req/s limit)
- Exponential backoff with jitter on HTTP 429 and 5xx (via tenacity)
- Descriptive User-Agent per MLB ToS
- Context-manager lifecycle for connection pooling

Usage:
    async with MLBClient() as client:
        data = await client.get("/v1/schedule", params={"sportId": 1, "season": 2024})
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = structlog.get_logger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api"
USER_AGENT = "MLB-DataPipeline/1.0 (MLB Data Engineering; dataeng@mlb.com)"
DEFAULT_RATE_RPS = 8.0
DEFAULT_MAX_RETRIES = 5


class TokenBucket:
    """
    Async token-bucket rate limiter.

    Refills at `rate` tokens/second up to `capacity`. Each call to
    `acquire()` consumes one token, sleeping if the bucket is empty.
    """

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self._rate = rate
        self._capacity = capacity if capacity is not None else rate
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self._capacity,
                self._tokens + (now - self._last) * self._rate,
            )
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient errors worth retrying."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return isinstance(exc, httpx.TimeoutException | httpx.ConnectError)


class MLBClient:
    """
    Async HTTP client for the MLB Stats API.

    Must be used as an async context manager so the underlying
    httpx.AsyncClient connection pool is properly managed.

        async with MLBClient() as client:
            schedule = await client.get("/v1/schedule", params={...})
    """

    def __init__(
        self,
        base_url: str = MLB_API_BASE,
        rate_rps: float = DEFAULT_RATE_RPS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._bucket = TokenBucket(rate=rate_rps)
        self._max_retries = max_retries
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> MLBClient:
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            ),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """
        GET a JSON endpoint, honouring the rate limit and retry policy.

        Args:
            path:   API path relative to base URL (e.g. "/v1/schedule")
            params: Query string parameters

        Returns:
            Parsed JSON response (dict or list)

        Raises:
            httpx.HTTPStatusError: On non-retryable HTTP errors (4xx except 429)
            httpx.TimeoutException: If all retries are exhausted
        """
        assert self._http is not None, "MLBClient must be used as 'async with MLBClient()'"

        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=1, max=60),
            reraise=True,
        )
        async def _fetch() -> Any:
            await self._bucket.acquire()
            log.debug("mlb_api_get", path=path, params=params)
            response = await self._http.get(path, params=params)  # type: ignore[union-attr]
            response.raise_for_status()
            return response.json()

        return await _fetch()
