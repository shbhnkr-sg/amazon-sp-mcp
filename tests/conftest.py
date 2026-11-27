import gzip
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# Must be set before `server` is imported (it resolves the marketplace at import).
os.environ.update(
    LWA_CLIENT_ID="test-client-id",
    LWA_CLIENT_SECRET="test-client-secret",
    LWA_REFRESH_TOKEN="test-refresh-token",
    MARKETPLACE_ID="A21TJRUUN4KGV",
)

import httpx  # noqa: E402
import pytest  # noqa: E402
import respx  # noqa: E402
from fastmcp import Client  # noqa: E402

import server  # noqa: E402
from sp_api.auth.access_token_client import cache, grantless_cache  # noqa: E402

EU = "https://sellingpartnerapi-eu.amazon.com"
LWA = "https://api.amazon.com/auth/o2/token"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def api():
    """respx router: LWA token endpoint mocked; any unmocked request fails the test
    (so a wrong host/endpoint or an unexpected extra call is caught)."""
    cache.clear()
    grantless_cache.clear()
    with respx.mock(assert_all_called=False) as router:
        router.lwa = router.post(LWA).mock(return_value=httpx.Response(
            200, json={"access_token": "tok-123", "refresh_token": "r", "token_type": "bearer",
                       "expires_in": 3600}))
        yield router


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(server.time, "sleep", lambda s: None)


@pytest.fixture(autouse=True)
def fresh_caches():
    """The terminal-shipment caches live for the process; each test starts empty."""
    for name in ("_SHIPMENT_CACHE", "_TRANSPORT_CACHE"):
        getattr(server, name, {}).clear()


@pytest.fixture
def doc_host():
    """Real local HTTP server standing in for the presigned S3 report-document URL
    (server.py downloads it with urllib, not httpx)."""
    docs = {}

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = docs[self.path]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}"

    def add(path, body, gz=False):
        docs[path] = gzip.compress(body) if gz else body
        return base + path

    yield add
    srv.shutdown()


async def call(tool, **args):
    """Call an MCP tool through an in-memory FastMCP client; return structured result."""
    async with Client(server.mcp) as c:
        res = await c.call_tool(tool, args)
    return res.structured_content


def q(route, call_idx=-1):
    """Query params of a recorded request, as a dict of lists."""
    return httpx.QueryParams(route.calls[call_idx].request.url.query).multi_items()


def params(route, call_idx=-1):
    out = {}
    for k, v in q(route, call_idx):
        out.setdefault(k, []).append(v)
    return out
