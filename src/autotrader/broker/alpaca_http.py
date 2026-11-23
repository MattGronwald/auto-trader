"""Alpaca trading-API transport (WP 1.6a spec 3.1, 3.2; PLAN D22 (1), (5), (9)).

Two `httpx.AsyncClient`s: GETs use a keep-alive read client, `POST /orders` and
`DELETE /orders/{id}` a write client that opens a fresh connection every time (no
keep-alive), so a silently dropped pooled connection can never swallow a write.

- Fixed timeouts (`TIMEOUT`), no retries of any kind (`retries=0`, no loop), no redirects,
  `trust_env=False` on the clients and on the hand-built transports (no env proxy, no
  `.netrc`, no `SSL_CERT_FILE`), TLS verification on, no cookies kept (a jar whose policy
  allows no domain).
- HTTP status codes are returned, never raised: the status contract lives in `alpaca.py`.
- Exceptions: a call on a closed client, `httpx.ConnectError`, `ConnectTimeout` and
  `PoolTimeout` raise `RequestNotSent` (nothing of the request was written: the connection
  never existed or none was handed out); any other `Exception` raises `BrokerError` (bytes
  may have reached the server). Messages carry the method, the path template and the cause's
  type name only, never its text, headers or keys, and every raise uses `from None`, so a
  rendered traceback has no cause chain. `asyncio.CancelledError` passes through.
- The `RequestNotSent` argument rests on httpcore/httpx internals verified for httpx
  0.28.1 / httpcore 1.0.9 / anyio 4.16.0 under these conditions: no proxy, HTTP/1.1 only,
  `retries=0`, the anyio backend (docs/alpaca-notes.md). Re-check on any upgrade.
- `ClosingBackend` wraps httpcore's anyio backend and closes the socket when a request is
  cancelled during the TLS handshake (httpcore closes it only on `Exception`). httpx has no
  public hook for a backend, so `make_transport` sets the pool's private `_network_backend`.
- One `alpaca.http` log line per call that returns or raises: method, path template, order
  id, status or error type, elapsed ms, `X-Request-ID`, and Alpaca's `message` for a non-2xx.
  `info` for a 2xx, `warning` otherwise. A call cancelled in flight (`CancelledError`, e.g.
  the engine's `wait_for` or a shutdown) writes none; WP 1.6d adds that line (PLAN 1.6 row).
- Response-derived text is filtered before it reaches a log field or an exception message:
  Alpaca's `message` through `sanitize`, every other value through `AlpacaHttp.diag` (shown
  only if it fully matches the pattern for its kind, then the keys redacted). The request id
  is filtered once, here, before it enters `HttpResult`. Comparisons use raw values.
"""

import re
import ssl
import time
import typing
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Any

import httpcore
import httpx
import structlog

from autotrader.broker.base import BrokerError, RequestNotSent

TIMEOUT = httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)
READ_LIMITS = httpx.Limits(max_connections=4, max_keepalive_connections=4)
WRITE_LIMITS = httpx.Limits(max_connections=4, max_keepalive_connections=0)
PAPER_URL = "https://paper-api.alpaca.markets/v2"
LIVE_URL = "https://api.alpaca.markets/v2"

# Shapes a response-derived diagnostic must fully match to be shown (`AlpacaHttp.diag`).
STATUS_RE = re.compile(r"[a-z_]{1,32}")  # Alpaca order statuses
REQUEST_ID_RE = re.compile(r"[A-Za-z0-9-]{1,64}")  # X-Request-ID
FIELD_RE = re.compile(r"[A-Za-z0-9/._:-]{1,64}")  # ack fields: ids, symbols, sides, decimals

REDACTED = "[redacted]"
MESSAGE_CAP = 200

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class HttpResult:
    status: int
    body: Any  # parsed JSON; None for an empty body or one that is not JSON
    parsed: bool  # True iff the body was non-empty, valid JSON
    request_id: str | None  # X-Request-ID, already filtered through `diag`


def wire_decimal(value: Decimal) -> str:
    """A decimal as Alpaca expects it: plain digits, never an exponent."""
    return format(value, "f")


def _redact(text: str, secrets: Sequence[str]) -> str:
    for secret in secrets:
        if secret:  # an empty secret would match between every character
            text = text.replace(secret, REDACTED)
    return text


def sanitize(text: object, secrets: Sequence[str]) -> str:
    """Alpaca's free-text `message`, fit for a log field or an exception message.

    `str(text)` with control and format characters removed, every occurrence of a secret
    replaced by `[redacted]`, capped at 200 characters plus `...`. Characters are removed
    before redacting, so a secret split by a control character is still caught; redacting
    happens before the cap, so the cap cannot leave a partial secret behind."""
    raw = str(text)
    cleaned = "".join(ch for ch in raw if unicodedata.category(ch) not in ("Cc", "Cf"))
    redacted = _redact(cleaned, secrets)
    if len(redacted) > MESSAGE_CAP:
        return redacted[:MESSAGE_CAP] + "..."
    return redacted


def _diag(value: object, pattern: re.Pattern[str], secrets: Sequence[str]) -> str:
    text = str(value)
    if pattern.fullmatch(text) is None:
        return f"<withheld: {len(text)} chars>"
    return _redact(text, secrets)


class _ClosingStream(httpcore.AsyncNetworkStream):
    """Delegates to the anyio stream; closes it when `start_tls` ends in any way but success."""

    def __init__(self, inner: httpcore.AsyncNetworkStream) -> None:
        self._inner = inner

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return await self._inner.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self._inner.write(buffer, timeout)

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            return await self._inner.start_tls(ssl_context, server_hostname, timeout)
        except BaseException:  # httpcore closes only on Exception; a cancel leaked the socket
            await self._inner.aclose()
            raise

    def get_extra_info(self, info: str) -> Any:
        return self._inner.get_extra_info(info)


class ClosingBackend(httpcore.AsyncNetworkBackend):
    """httpcore's anyio backend plus the close on a cancelled TLS handshake (D22 (9)).

    Resolves nothing itself: DNS and TCP stay with `httpcore.AnyIOBackend`."""

    def __init__(self) -> None:
        self._inner = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        stream = await self._inner.connect_tcp(host, port, timeout, local_address, socket_options)
        return _ClosingStream(stream)

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise NotImplementedError("the trading API is reached over TCP only")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


def make_transport(limits: httpx.Limits) -> httpx.AsyncHTTPTransport:
    """The production transport: no retries, TLS verify on, env ignored, HTTP/1.1, and the
    closing backend on the pool's private `_network_backend` (no public hook in httpx 0.28;
    pinned by the loopback test, re-check on upgrade)."""
    transport = httpx.AsyncHTTPTransport(
        limits=limits, retries=0, verify=True, trust_env=False, http2=False
    )
    transport._pool._network_backend = ClosingBackend()
    return transport


def _no_cookies() -> CookieJar:
    # Passed raw: wrapping it in httpx.Cookies would copy it into a jar without the policy.
    return CookieJar(policy=DefaultCookiePolicy(allowed_domains=[]))


class AlpacaHttp:
    """The two trading-API clients.

    `transport` is for tests only; one transport serves both clients, so the write client
    then keeps alive whatever that transport keeps alive. Without it each client gets its
    own `make_transport` (read: keep-alive pool, write: no keep-alive)."""

    def __init__(
        self,
        *,
        base_url: str,
        key_id: str,
        secret_key: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: httpx.Timeout = TIMEOUT,
    ) -> None:
        self._secrets = (key_id, secret_key)
        headers = {"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret_key}

        def client(limits: httpx.Limits) -> httpx.AsyncClient:
            return httpx.AsyncClient(
                base_url=base_url,
                headers=headers,
                transport=transport if transport is not None else make_transport(limits),
                timeout=timeout,
                trust_env=False,
                follow_redirects=False,
                cookies=_no_cookies(),
            )

        self._read = client(READ_LIMITS)
        self._write = client(WRITE_LIMITS)

    def sanitize_message(self, text: object) -> str:
        """`sanitize` bound to this client's keys, for Alpaca's `message`."""
        return sanitize(text, self._secrets)

    def diag(self, value: object, pattern: re.Pattern[str]) -> str:
        """A response-derived value for a log field or an exception message: shown only if
        `str(value)` fully matches `pattern`, else `<withheld: N chars>`; the keys are then
        redacted (the key id alone would pass `REQUEST_ID_RE`)."""
        return _diag(value, pattern, self._secrets)

    async def get(
        self,
        template: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        order_id: str | None = None,
    ) -> HttpResult:
        return await self._send(self._read, "GET", template, path, order_id=order_id, params=params)

    async def post(self, template: str, path: str, payload: Mapping[str, str]) -> HttpResult:
        return await self._send(self._write, "POST", template, path, json=payload)

    async def delete(self, template: str, path: str, *, order_id: str | None = None) -> HttpResult:
        return await self._send(self._write, "DELETE", template, path, order_id=order_id)

    async def aclose(self) -> None:
        """Close both clients (and with them every pooled connection)."""
        try:
            await self._read.aclose()
        finally:
            await self._write.aclose()

    @property
    def closed(self) -> bool:
        return self._read.is_closed and self._write.is_closed

    async def _send(
        self,
        client: httpx.AsyncClient,
        method: str,
        template: str,
        path: str,
        *,
        order_id: str | None = None,
        params: Mapping[str, str] | None = None,
        json: Mapping[str, str] | None = None,
    ) -> HttpResult:
        fields: dict[str, Any] = {"method": method, "path": template}
        if order_id is not None:
            fields["order_id"] = order_id
        if client.is_closed:  # before any I/O: a late caller after teardown sends nothing
            log.warning(
                "alpaca.http", **fields, error="ClientClosed", elapsed_ms=0, request_id=None
            )
            raise RequestNotSent(f"alpaca {method} {template} not sent: client closed")
        start = time.monotonic()
        try:
            response = await client.request(method, path, params=params, json=json)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
            cause = type(e).__name__
            log.warning(
                "alpaca.http", **fields, error=cause, elapsed_ms=_ms(start), request_id=None
            )
            raise RequestNotSent(f"alpaca {method} {template} not sent: {cause}") from None
        except Exception as e:  # CancelledError is a BaseException and propagates
            cause = type(e).__name__
            log.warning(
                "alpaca.http", **fields, error=cause, elapsed_ms=_ms(start), request_id=None
            )
            raise BrokerError(f"alpaca {method} {template} failed: {cause}") from None
        result = self._result(response)
        fields.update(status=result.status, elapsed_ms=_ms(start), request_id=result.request_id)
        if 200 <= result.status < 300:
            log.info("alpaca.http", **fields)
        else:
            message = (
                self.sanitize_message(result.body.get("message"))
                if isinstance(result.body, dict)
                else None
            )
            log.warning("alpaca.http", **fields, message=message)
        return result

    def _result(self, response: httpx.Response) -> HttpResult:
        body: Any = None
        parsed = False
        if response.content:
            try:
                body = response.json()
                parsed = True
            # JSONDecodeError and UnicodeDecodeError are ValueErrors; absurdly nested JSON
            # raises RecursionError. Either way: not JSON, never an exception.
            except (ValueError, RecursionError):
                body = None
        raw_id = response.headers.get("X-Request-ID")
        return HttpResult(
            status=response.status_code,
            body=body,
            parsed=parsed,
            request_id=None if raw_id is None else self.diag(raw_id, REQUEST_ID_RE),
        )


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
