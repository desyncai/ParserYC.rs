from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import httpx
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from Python_Scraping.core.logging import warn


@dataclass
class BulkResult:
    pages: List
    attempted: int
    wait_time: Optional[int]
    extract_html: bool
    error: Optional[Exception] = None


class DesyncScraper:
    """Wrapper around desync_search.DesyncClient with retries and chunking."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        default_wait_time: Optional[int] = None,
        default_extract_html: bool = False,
        client=None,
        http_client: Optional[httpx.Client] = None,
        max_attempts: int = 3,
    ) -> None:
        self.api_key = api_key or os.getenv("DESYNC_API_KEY")
        if not self.api_key:
            raise ValueError(
                "DESYNC_API_KEY is missing; set it in your env or pass api_key="
            )
        self.default_wait_time = default_wait_time
        self.default_extract_html = default_extract_html
        self._client = client
        self.http = http_client or httpx.Client(timeout=30)
        self.max_attempts = max_attempts

    @property
    def client(self):  # lazy import to keep startup fast
        if self._client is None:
            from desync_search import DesyncClient

            self._client = DesyncClient(user_api_key=self.api_key)
        return self._client

    def _retry_bulk(self, payload: dict) -> List:
        @retry(
            wait=wait_exponential(multiplier=1, min=1, max=10),
            stop=stop_after_attempt(self.max_attempts),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def _call():
            return self.client.simple_bulk_search(**payload)

        return _call()

    def bulk_search(
        self,
        urls: Sequence[str],
        *,
        wait_time: Optional[int] = None,
        extract_html: Optional[bool] = None,
    ) -> BulkResult:
        if not urls:
            return BulkResult(
                pages=[],
                attempted=0,
                wait_time=wait_time,
                extract_html=bool(extract_html),
            )

        payload = {
            "target_list": list(dict.fromkeys(urls)),  # dedupe preserving order
            "extract_html": extract_html
            if extract_html is not None
            else self.default_extract_html,
        }
        final_wait = wait_time if wait_time is not None else self.default_wait_time
        if final_wait is not None:
            payload["wait_time"] = final_wait

        try:
            pages = self._retry_bulk(payload)
            return BulkResult(
                pages=pages,
                attempted=len(urls),
                wait_time=final_wait,
                extract_html=payload["extract_html"],
            )
        except RetryError as exc:  # expose final error but keep attempted count
            warn(f"Bulk search retry exhausted: {exc}")
            last_error = exc.last_attempt.exception() if exc.last_attempt else exc
            return BulkResult(
                pages=[],
                attempted=len(urls),
                wait_time=final_wait,
                extract_html=payload["extract_html"],
                error=last_error,
            )

    def chunked_bulk_search(
        self,
        urls: Iterable[str],
        *,
        chunk_size: int = 100,
        wait_time: Optional[int] = None,
        extract_html: Optional[bool] = None,
    ) -> Iterable[BulkResult]:
        batch: List[str] = []
        for url in urls:
            batch.append(url)
            if len(batch) >= chunk_size:
                yield self.bulk_search(
                    batch, wait_time=wait_time, extract_html=extract_html
                )
                batch = []
        if batch:
            yield self.bulk_search(
                batch, wait_time=wait_time, extract_html=extract_html
            )

    def ping(self) -> bool:
        try:
            resp = self.http.get("https://example.com", timeout=5)
            return resp.status_code < 500
        except Exception:
            return False
