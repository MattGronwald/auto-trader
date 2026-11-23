"""Recorded Alpaca responses and a routing `httpx.MockTransport` for the adapter tests.

`probe()` reads `tests/fixtures/alpaca/order_probes_20261112.json` (paper probes, WP 1.6a);
`Router` answers queued responses per (method, path) and records every request. A request
with no queued response is recorded in `unrouted` and fails loudly, so a mis-keyed route
can never pass as a broker error (review I6).
"""

import json
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

from autotrader.broker.base import BrokerOrder

KEY, SECRET = "KEYID-not-a-real-key-7", "SECRET-not-a-real-secret-9"
PROBES = Path(__file__).parents[2] / "fixtures" / "alpaca" / "order_probes_20261112.json"
_LOG: dict[str, list[dict[str, Any]]] = {
    run: data["log"] for run, data in json.loads(PROBES.read_text()).items() if run != "_note"
}

# The account shape recorded from the paper API on 2026-10-11 (decimals are strings).
ACCOUNT: dict[str, Any] = {
    "status": "ACTIVE",
    "crypto_status": "ACTIVE",
    "currency": "USD",
    "equity": "100000.55",
    "cash": "99000.10",
    "crypto_tier": 1,
}


def probe(step: str, *, run: str = "round1", n: int = 0) -> dict[str, Any]:
    """The n-th logged entry for `step` in probe run `run`."""
    return [entry for entry in _LOG[run] if entry["step"] == step][n]


def order_from(body: dict[str, str]) -> BrokerOrder:
    """The `BrokerOrder` a probe sent (its request body)."""
    limit = body.get("limit_price")
    return BrokerOrder.model_validate(
        {
            "symbol": body["symbol"],
            "side": body["side"],
            "type": body["type"],
            "qty": Decimal(body["qty"]),
            "limit_price": None if limit is None else Decimal(limit),
            "time_in_force": body["time_in_force"],
            "client_order_id": body.get("client_order_id"),
        }
    )


@dataclass(frozen=True)
class _Route:
    status: int
    json: Any
    content: bytes | None
    repeat: bool
    raises: type[httpx.TransportError] | None


class Router:
    """Queued responses per (method, path); `path` includes `/v2`, not the query.

    Every response carries `X-Request-ID: rid-<n>`, n the 1-based request count."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.unrouted: list[str] = []
        self._routes: dict[tuple[str, str], deque[_Route]] = {}
        self.transport = httpx.MockTransport(self._handle)

    def add(
        self,
        method: str,
        path: str,
        status: int = 200,
        json: Any = None,
        *,
        content: bytes | None = None,
        repeat: bool = False,
        raises: type[httpx.TransportError] | None = None,
    ) -> None:
        route = _Route(status, json, content, repeat, raises)
        self._routes.setdefault((method, path), deque()).append(route)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self._routes.get((request.method, request.url.path))
        if not queue:
            self.unrouted.append(f"{request.method} {request.url.path}")
            raise RuntimeError(f"unrouted {request.method} {request.url.path}")
        route = queue[0] if queue[0].repeat else queue.popleft()
        if route.raises is not None:
            raise route.raises("routed transport error", request=request)
        headers = {"X-Request-ID": f"rid-{len(self.requests)}"}
        if route.content is not None:
            return httpx.Response(route.status, content=route.content, headers=headers)
        if route.json is None:
            return httpx.Response(route.status, headers=headers)
        return httpx.Response(route.status, json=route.json, headers=headers)
