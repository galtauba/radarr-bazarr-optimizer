# -*- coding: utf-8 -*-

import random
import time
from typing import Any, Dict, List, Optional

import requests

from optimizer_app.logging_utils import logger


class HttpClient:
    def __init__(
        self,
        *,
        timeout: int,
        retries: int,
        backoff_seconds: int,
        verify_ssl: bool,
        user_agent: str,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        allow_statuses: Optional[List[int]] = None,
    ) -> requests.Response:
        allow_statuses = allow_statuses or []
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )

                if resp.status_code in allow_statuses:
                    return resp
                if 200 <= resp.status_code < 300:
                    return resp

                if resp.status_code in (408, 409, 425, 429, 500, 502, 503, 504):
                    sleep_for = self.backoff_seconds * attempt + random.uniform(0, 0.4)
                    logger.warning(
                        "HTTP %s %s returned %s (attempt %s/%s), retrying in %.1fs",
                        method.upper(),
                        url,
                        resp.status_code,
                        attempt,
                        self.retries,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    continue

                raise requests.HTTPError(
                    f"{method.upper()} {url} failed with {resp.status_code}: {resp.text[:500]}",
                    response=resp,
                )
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                if attempt >= self.retries:
                    break
                sleep_for = self.backoff_seconds * attempt + random.uniform(0, 0.4)
                logger.warning(
                    "HTTP %s %s failed on attempt %s/%s: %s; retrying in %.1fs",
                    method.upper(),
                    url,
                    attempt,
                    self.retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)

        raise RuntimeError(
            f"HTTP request failed after {self.retries} attempts: {method.upper()} {url} | {last_exc}"
        )

