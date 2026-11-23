"""The trading-API transport `broker/alpaca_http.py` (WP 1.6a spec 3.1, 3.2).

`httpx.MockTransport` for the client settings, the error mapping and the log line; asyncio
servers on 127.0.0.1 (`loopback.py`) for what only the real httpx/httpcore stack shows.
"""

import asyncio
import importlib.metadata
import io
import ssl
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

import httpcore
import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from autotrader.broker.alpaca_http import (
    MESSAGE_CAP,
    PAPER_URL,
    REQUEST_ID_RE,
    STATUS_RE,
    AlpacaHttp,
    ClosingBackend,
    sanitize,
    wire_decimal,
)
from autotrader.broker.base import BrokerError, RequestNotSent
from autotrader.core.logging import configure_logging
from tests.unit.broker.loopback import KeepAliveHttp, SilentTls, free_port

KEY, SECRET = "KEYID-not-a-real-key-7", "SECRET-not-a-real-secret-9"
Handler = (
    Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]
)


def http(handler: Handler, **kw: Any) -> AlpacaHttp:
    return AlpacaHttp(
        base_url=PAPER_URL,
        key_id=KEY,
        secret_key=SECRET,
        transport=httpx.MockTransport(handler),
        **kw,
    )


async def test_clients_carry_the_specified_settings() -> None:
    h = http(lambda r: httpx.Response(200, json={}))
    for c in (h._read, h._write):  # private access in tests only
        assert c.timeout == httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)
        assert c.follow_redirects is False and c.trust_env is False
        assert c.headers["APCA-API-KEY-ID"] == KEY
    await h.aclose()


async def test_production_transports_ignore_env_and_never_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # review I2, M7: the settings MockTransport hides
    monkeypatch.setenv("SSL_CERT_FILE", "/nonexistent/ca.pem")  # trust_env would load it
    h = AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET)
    for client, keepalive in ((h._read, 4), (h._write, 0)):
        pool = client._transport._pool  # type: ignore[attr-defined]
        # no proxy (spec 3.2 condition): a proxy pool subclasses AsyncConnectionPool, so the
        # type must be exact, and no env proxy is mounted (final review M3)
        assert type(pool) is httpcore.AsyncConnectionPool
        assert client._mounts == {}
        assert pool._retries == 0 and pool._http2 is False
        assert pool._max_keepalive_connections == keepalive
        assert pool._ssl_context is not None
        assert pool._ssl_context.verify_mode is ssl.CERT_REQUIRED
        assert pool._ssl_context.check_hostname is True
        assert isinstance(pool._network_backend, ClosingBackend)
    await h.aclose()


# The versions whose sources the `RequestNotSent` argument was verified against (spec D22,
# docs/alpaca-notes.md "Why a connect failure proves 'not sent'"). Fable review 2 M3.
VERIFIED_TRANSPORT_VERSIONS = {
    "httpx": "0.28.1",
    "httpcore": "1.0.9",
    "anyio": "4.16.0",
    "h11": "0.16.0",
}


def test_the_transport_runs_on_the_versions_the_not_sent_proof_was_verified_on() -> None:
    installed = {p: importlib.metadata.version(p) for p in VERIFIED_TRANSPORT_VERSIONS}
    assert installed == VERIFIED_TRANSPORT_VERSIONS, (
        "httpx/httpcore/anyio/h11 changed: before updating VERIFIED_TRANSPORT_VERSIONS, "
        "re-verify the ConnectError/ConnectTimeout/PoolTimeout raise sites and the other "
        "facts in docs/alpaca-notes.md ('Why a connect failure proves \"not sent\"'; "
        "WP 1.6a spec 3.2, PLAN D22). If one no longer holds, RequestNotSent is unsafe."
    )


async def test_a_set_cookie_is_not_kept() -> None:
    h = http(lambda r: httpx.Response(200, json={}, headers={"set-cookie": "a=b; Path=/"}))
    await h.get("/account", "/account")
    await h.post("/orders", "/orders", {})  # the write client sees a Set-Cookie too
    assert len(h._read.cookies.jar) == 0 and len(h._write.cookies.jar) == 0
    await h.aclose()


@pytest.mark.parametrize("status", [429, 503])
async def test_no_retry_on_status(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        return httpx.Response(status, json={"message": "x"})

    result = await http(handler).post("/orders", "/orders", {"symbol": "BTC/USD"})
    assert (result.status, len(seen)) == (status, 1)


@pytest.mark.parametrize(
    ("exc", "raised"),
    [
        (httpx.ConnectError, RequestNotSent),
        (httpx.ConnectTimeout, RequestNotSent),
        (httpx.PoolTimeout, RequestNotSent),
        (httpx.WriteError, BrokerError),
        (httpx.WriteTimeout, BrokerError),
        (httpx.ReadError, BrokerError),
        (httpx.ReadTimeout, BrokerError),
        (httpx.RemoteProtocolError, BrokerError),
    ],
)
async def test_transport_errors_map_by_whether_bytes_may_have_left(
    exc: type[httpx.TransportError], raised: type[BrokerError]
) -> None:
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        raise exc("boom", request=r)

    with pytest.raises(raised) as e:
        await http(handler).post("/orders", "/orders", {})
    assert type(e.value) is raised and len(seen) == 1  # exact class, no retry
    assert KEY not in str(e.value) and SECRET not in str(e.value)


async def test_a_rendered_exception_log_carries_no_cause_text_or_secret() -> None:  # Codex P2-1
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadError(f"MARKER-cause {SECRET}", request=r)

    buf = io.StringIO()
    configure_logging(stream=buf)  # production chain; tests/conftest.py restores the config
    try:
        await http(handler).post("/orders", "/orders", {})
    except BrokerError:
        structlog.get_logger("t").exception("risk.order_unknown")
    rendered = buf.getvalue()
    assert "ReadError" in rendered and "risk.order_unknown" in rendered
    assert "MARKER-cause" not in rendered and SECRET not in rendered and KEY not in rendered


async def test_alpaca_message_text_is_sanitised() -> None:  # Codex P2-1
    echoed = f"bad key {KEY} / {SECRET}\n" + "x" * 500
    h = http(lambda r: httpx.Response(403, json={"message": echoed}))
    with capture_logs() as logs:
        await h.post("/orders", "/orders", {})
    (line,) = [x for x in logs if x["event"] == "alpaca.http"]
    assert KEY not in line["message"] and SECRET not in line["message"]
    assert "[redacted]" in line["message"] and "\n" not in line["message"]
    assert len(line["message"]) <= 203


async def test_response_derived_diagnostics_are_filtered() -> None:  # Codex r2
    h = http(lambda r: httpx.Response(500, json={}, headers={"X-Request-ID": f"rid {SECRET}"}))
    with capture_logs() as logs:
        result = await h.get("/account", "/account")
    assert result.request_id == "<withheld: " + str(len(f"rid {SECRET}")) + " chars>"
    assert SECRET not in repr(logs)
    assert h.diag("held", STATUS_RE) == "held"
    assert h.diag(f"held{SECRET}", STATUS_RE).startswith("<withheld")
    assert h.diag(KEY, REQUEST_ID_RE) == "[redacted]"  # the pattern alone would let it through


@pytest.mark.parametrize("error", [OSError("reset"), RuntimeError("odd")])
async def test_any_other_exception_is_a_broker_error(error: Exception) -> None:  # review I7
    def handler(r: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(BrokerError) as e:
        await http(handler).get("/account", "/account")
    assert type(e.value) is BrokerError


async def test_a_closed_client_is_request_not_sent() -> None:  # review I7
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        return httpx.Response(200, json={})

    h = http(handler)
    await h.aclose()
    assert h.closed
    with pytest.raises(RequestNotSent, match="client closed"):
        await h.post("/orders", "/orders", {})
    assert seen == []


def test_sanitize_removes_format_characters_before_redacting() -> None:
    # A secret split by a zero-width space (category Cf) is still a secret once joined.
    split = SECRET[:5] + "\u200b" + SECRET[5:]
    text = sanitize(f"bad {split} end", (KEY, SECRET))
    assert text == "bad [redacted] end"


def test_sanitize_redacts_before_capping() -> None:
    # A secret straddling the cap: capping first would keep its first part.
    pad = "x" * (MESSAGE_CAP - 10)
    text = sanitize(pad + SECRET + "tail", (KEY, SECRET))
    assert SECRET[:10] not in text
    assert text == (pad + "[redacted]tail")[:MESSAGE_CAP] + "..."


def test_decimals_go_out_without_exponent() -> None:
    assert wire_decimal(Decimal("1E-7")) == "0.0000001"
    assert wire_decimal(Decimal("73667.30052")) == "73667.30052"


async def test_one_log_line_per_call_without_secrets() -> None:
    responses = iter(
        [
            httpx.Response(403, json={"message": "cost basis"}, headers={"X-Request-ID": "rid-1"}),
            httpx.Response(200, json={}, headers={"X-Request-ID": "rid-2"}),
        ]
    )
    h = http(lambda r: next(responses))
    with capture_logs() as logs:
        await h.post("/orders", "/orders", {})
        await h.get("/orders/{id}", "/orders/o-1", order_id="o-1")
    lines = [line for line in logs if line["event"] == "alpaca.http"]
    assert [
        (x["method"], x["path"], x.get("order_id"), x["status"], x["log_level"], x["request_id"])
        for x in lines
    ] == [
        ("POST", "/orders", None, 403, "warning", "rid-1"),
        ("GET", "/orders/{id}", "o-1", 200, "info", "rid-2"),  # 2xx reads log at info (M5)
    ]
    assert lines[0]["message"] == "cost basis"
    assert KEY not in repr(logs) and SECRET not in repr(logs)


async def test_a_handler_blocked_past_cancellation_records_nothing() -> None:
    # Proves only that AlpacaHttp neither shields nor detaches the request (spec wording);
    # the loopback test below carries the real claim (review M8).
    started, release = asyncio.Event(), asyncio.Event()
    completed: list[httpx.Request] = []

    async def handler(r: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        completed.append(r)
        return httpx.Response(200, json={})

    h = http(handler)
    task = asyncio.create_task(h.post("/orders", "/orders", {}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.sleep(0)
    assert completed == []


@pytest.mark.parametrize("how", ["cancel", "wait_for"])
async def test_a_request_cancelled_in_tls_setup_writes_nothing_and_closes(how: str) -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(base_url=f"https://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        call = h.post("/orders", "/orders", {"symbol": "BTC/USD"})
        if how == "cancel":
            task = asyncio.create_task(call)
            async with asyncio.timeout(10):
                await srv.hello.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:  # the engine's path: wait_for times the caller out during the handshake
            task = asyncio.create_task(asyncio.wait_for(call, 3600))
            async with asyncio.timeout(10):
                await srv.hello.wait()
            task.cancel()  # stands in for the deadline firing; same cancellation inside wait_for
            with pytest.raises(asyncio.CancelledError):
                await task
        async with asyncio.timeout(10):
            await srv.closed.wait()  # EOF before aclose: the backend closed the socket (C1)
        await h.aclose()
    assert srv.record_types() == {0x16}  # TLS handshake records only, no application data
    assert b"POST" not in srv.received and SECRET.encode() not in srv.received


async def test_a_connect_timeout_and_a_refused_port_are_request_not_sent() -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(
            base_url=f"https://127.0.0.1:{srv.port}/v2",
            key_id=KEY,
            secret_key=SECRET,
            timeout=httpx.Timeout(5.0, connect=0.05),
        )
        with pytest.raises(RequestNotSent, match="ConnectTimeout"):
            await h.post("/orders", "/orders", {})
        await h.aclose()
    port = free_port()
    h = AlpacaHttp(base_url=f"http://127.0.0.1:{port}/v2", key_id=KEY, secret_key=SECRET)
    with pytest.raises(RequestNotSent, match="ConnectError"):
        await h.post("/orders", "/orders", {})
    await h.aclose()


async def test_writes_open_a_fresh_connection_reads_reuse_one() -> None:
    async with KeepAliveHttp() as srv:
        h = AlpacaHttp(base_url=f"http://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        await h.post("/orders", "/orders", {})
        await h.post("/orders", "/orders", {})
        assert srv.connections == 2
        await h.get("/account", "/account")
        await h.get("/account", "/account")
        assert srv.connections == 3
        oid = "00000000-0000-4000-8000-00000016a009"
        for _ in range(2):  # a cancel is a write too (spec 3.1, Fable review 2 M6)
            await h.delete("/orders/{id}", f"/orders/{oid}", order_id=oid)
        assert srv.connections == 5
        await h.aclose()
    assert h.closed
