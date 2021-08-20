# from https://gist.github.com/haizaar/63b941ec747f71d076494847fef49317
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Hashable, List, Optional

import google.auth
import google.auth.credentials
import httplib2
import structlog
from googleapiclient.http import HttpRequest

logger = structlog.get_logger(__name__)


@dataclass
class MemCache:
    data: dict[Hashable, Any] = field(default_factory=dict)

    def get(self, key: Hashable) -> Any:
        if hit := self.data.get(key, None):
            logger.debug("Cache hit", key=key)
        return hit

    def set(self, key: Hashable, data: Any) -> None:
        self.data[key] = data

    def delete(self, key):
        try:
            del self.data[1]
        except KeyError:
            pass


@dataclass
class APIConnector:
    """
    This class is a thread-safe wrapper around HttpRequest.execute() method.
    It uses a pool of AuthorizedHttp objects and makes sure there is
    only one in-flight request for each.
    """

    factory: Callable[[], httplib2.Http]
    pool: List[httplib2.Http] = field(default_factory=list)

    @classmethod
    def new(
        cls,
        initial_size: int = 5,
        timeout_seconds: int = 3,
        cache: Optional[MemCache] = None,
    ) -> APIConnector:

        def factory(): return httplib2.Http(
            timeout=timeout_seconds, cache=cache
        )

        pool: List[httplib2.Http] = []
        for _ in range(initial_size):
            pool.append(factory())

        return cls(factory, pool=pool)

    def execute(self, request: HttpRequest) -> Any:
        http: Optional[httplib2.Http] = None
        try:
            http = self._provision_http()
            return request.execute(http=http)
        finally:
            if http:
                self.pool.append(http)

    def _provision_http(self) -> httplib2.Http:
        # This function can run in parallel in multiple threads.
        try:
            return self.pool.pop()
        except IndexError:
            logger.info("Transport pool exhausted. Creating new transport")
            return self.factory()

    def close(self) -> None:
        for ahttp in self.pool:
            ahttp.close()

    def __del__(self) -> None:
        self.close()
