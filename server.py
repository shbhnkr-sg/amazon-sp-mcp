import asyncio
import gzip
import inspect
import json
import logging
import os
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial, wraps
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from sp_api.api import (
    Orders,
    Inventories,
    Sales,
    Reports,
    CatalogItems,
    ProductFees,
)
from sp_api.api.finances.finances_v0 import FinancesV0
from sp_api.base import Marketplaces, Granularity
from sp_api.base.exceptions import SellingApiException

# httpx is a transitive dependency of python-amazon-sp-api; guard the import so a
# requests-based build still loads (we just retry fewer transport errors then).
try:
    import httpx
    _RETRY_NETWORK = (httpx.TransportError, json.JSONDecodeError)
except Exception:  # pragma: no cover - depends on installed transport
    _RETRY_NETWORK = (json.JSONDecodeError,)

load_dotenv()

log = logging.getLogger("amazon-sp")

mcp = FastMCP("amazon-sp")

# HTTP status codes worth retrying with backoff: throttle (429) + transient 5xx.
_RETRY_CODES = {429, 500, 502, 503, 504}
# Report types whose document is a JSON object (no TSV row/column model).
_JSON_REPORTS = {"GET_SALES_AND_TRAFFIC_REPORT"}
_MAX_JSON_CHARS = 60000


# --- Credentials & marketplace ------------------------------------------------

def creds():
    return dict(
        refresh_token=os.environ["LWA_REFRESH_TOKEN"],
        lwa_app_id=os.environ["LWA_CLIENT_ID"],
        lwa_client_secret=os.environ["LWA_CLIENT_SECRET"],
    )


def _resolve_marketplace(marketplace_id: str) -> Marketplaces:
    """Map a marketplace ID string to the Marketplaces enum the API classes need.

    python-amazon-sp-api expects `marketplace=` to be a Marketplaces enum member
    (it reads `.endpoint` / `.region` / `.marketplace_id` off it), NOT a raw ID
    string. Body params (marketplaceIds, granularityId, MarketplaceId) still use
    the string id, so we keep both forms below.
    """
    for m in Marketplaces:
        if m.marketplace_id == marketplace_id:
            return m
    raise ValueError(
        f"Unknown MARKETPLACE_ID {marketplace_id!r}: no matching sp_api Marketplaces member."
    )


MARKETPLACE_ENUM = _resolve_marketplace(os.getenv("MARKETPLACE_ID", "A21TJRUUN4KGV"))
MARKETPLACE = MARKETPLACE_ENUM.marketplace_id  # string id, for request body params


def _iso(dt):
    """SP-API-safe ISO8601 timestamp: no microseconds, 'Z' suffix (dt must be UTC).

    The Orders API rejects Python's default isoformat() ('+00:00'/microseconds)
    with 'timestamp must follow ISO8601'; this 'YYYY-MM-DDTHH:MM:SSZ' form works.
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# --- Shared helpers (retry, pagination, projection, time windows, errors) ------

class _Bucket:
    """Token bucket for one SP-API operation, shared by every thread and tool call.

    A caller reserves a token and gets back how long to wait before sending, so the
    lock is never held while sleeping and no one spins. ``drain`` empties it after a
    429, so every concurrent caller backs off together."""

    def __init__(self, rate, burst, clock=time.monotonic):
        self.rate, self.burst, self.clock = rate, burst, clock
        self.tokens, self.t = float(burst), clock()
        self.lock = threading.Lock()

    def _refill(self):
        now = self.clock()
        self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
        self.t = now

    def reserve(self):
        with self.lock:
            self._refill()
            self.tokens -= 1
            return max(0.0, -self.tokens / self.rate)

    def drain(self):
        with self.lock:
            self._refill()
            self.tokens = min(self.tokens, 0.0)


# Documented default usage plans (requests/second, burst), developer-docs.amazon/sp-api.
RATE_LIMITS = {
    # Fulfillment Inbound v2024-03-20
    "listInboundPlans": (2, 6),
    "getInboundPlan": (2, 6),
    "getShipment": (2, 6),
    "listShipmentItems": (2, 30),
    "listTransportationOptions": (2, 6),
    # Fulfillment Inbound v0
    "getShipmentItemsByShipmentId": (2, 30),
    # Reports 2021-06-30
    "createReport": (0.0167, 15),
    "getReport": (2, 15),
    "getReports": (0.0222, 10),
    "getReportDocument": (0.0167, 15),
    # Product Pricing v0
    "getPricing": (0.5, 1),
}
_BUCKETS = {op: _Bucket(r, b) for op, (r, b) in RATE_LIMITS.items()}
# A 429 is retried until this much backoff has been spent (it never counts toward
# max_attempts): under the 240 s probe and 300 s pipeline socket timeouts.
_THROTTLE_BUDGET = 120
_THROTTLE_CAP = 30


def _with_retry(call, *, max_attempts=5, base=1.0, op=None):
    """Invoke an SP-API thunk, retrying throttle/5xx/transient errors with backoff.

    ``op`` names the SP-API operation: each attempt first takes a token from its
    bucket (RATE_LIMITS). A 429 drains the bucket and is retried with backoff (capped
    at _THROTTLE_CAP s) until _THROTTLE_BUDGET s of backoff is spent. 5xx and
    transient transport/JSON errors are retried up to ``max_attempts``; everything
    else (4xx, bad input) is re-raised immediately.
    """
    bucket = _BUCKETS.get(op)
    attempt, throttles, waited = 0, 0, 0.0
    while True:
        if bucket:
            wait = bucket.reserve()
            if wait:
                time.sleep(wait)
        try:
            return call()
        except Exception as e:
            code = getattr(e, "code", None) if isinstance(e, SellingApiException) else None
            if code == 429:
                if bucket:
                    bucket.drain()
                delay = min(_THROTTLE_CAP, base * (2 ** throttles))
                if waited + delay > _THROTTLE_BUDGET:
                    raise
                throttles, waited = throttles + 1, waited + delay
                time.sleep(delay)
                continue
            attempt += 1
            retryable = code in _RETRY_CODES or (
                not isinstance(e, SellingApiException) and isinstance(e, _RETRY_NETWORK))
            if not retryable or attempt >= max_attempts:
                raise
            time.sleep(base * (2 ** (attempt - 1)))


def _paginate(call, items_key, get_token, *, max_items=200, max_pages=20):
    """Follow a NextToken-style cursor and accumulate items.

    ``call(token)`` receives ``None`` on the first page and the cursor thereafter;
    the caller must send full filters only on the first page (Amazon rejects a
    token combined with other filters). Each call is wrapped in ``_with_retry``.
    Returns ``(items, truncated, next_token)``. ``max_items`` is a soft cap:
    results are page-aligned, so the count may slightly exceed it.
    """
    items, token = [], None
    for _ in range(max_pages):
        payload = _with_retry(lambda: call(token)) or {}
        items.extend(payload.get(items_key) or [])
        token = get_token(payload)
        if not token or len(items) >= max_items:
            break
    return items, bool(token), token


def _project(items, fields):
    """Keep only the listed top-level keys per item (token-budget control)."""
    if not fields:
        return items
    keys = [f.strip() for f in fields.split(",") if f.strip()]
    return [{k: it[k] for k in keys if k in it} for it in items]


def _window(days_ago=None, after=None, before=None, *, floor_to_day=False,
            tz="Asia/Kolkata"):
    """Return (start, end) as SP-API UTC 'Z' strings.

    Bare dates/datetimes in ``after``/``before`` are interpreted in ``tz`` (the
    marketplace is IST); otherwise the window is the last ``days_ago`` days.
    ``floor_to_day`` snaps both ends to IST midnight so Sales buckets are whole
    days rather than ragged partial ones.
    """
    zone = ZoneInfo(tz)

    def parse(v):
        d = datetime.fromisoformat(v)
        return d.replace(tzinfo=zone) if d.tzinfo is None else d

    now_local = datetime.now(zone)
    start = parse(after) if after else now_local - timedelta(days=days_ago or 0)
    end = parse(before) if before else now_local
    if floor_to_day:
        # Floor in IST regardless of the input's tz, so buckets land on IST midnight.
        midnight = dict(hour=0, minute=0, second=0, microsecond=0)
        start = start.astimezone(zone).replace(**midnight)
        end = end.astimezone(zone).replace(**midnight)
    return _iso(start.astimezone(timezone.utc)), _iso(end.astimezone(timezone.utc))


def _error_envelope(fn, e):
    if isinstance(e, SellingApiException):
        return {"error": {"type": type(e).__name__,
                          "code": getattr(e, "code", None), "message": str(e)}}
    log.exception("tool %s failed", fn.__name__)
    return {"error": {"type": type(e).__name__, "message": str(e)}}


def tool_safe(fn):
    """Wrap a tool (sync or async) so any exception becomes a structured error envelope.

    Catches everything (not just SellingApiException, so KeyError/httpx/JSON no
    longer escape as raw 500s) and logs unexpected failures server-side so real
    bugs aren't silently swallowed. Keep ``@mcp.tool()`` above this decorator.
    """
    if inspect.iscoroutinefunction(fn):
        @wraps(fn)
        async def awrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001 - deliberate catch-all for tool safety
                return _error_envelope(fn, e)

        return awrapper

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 - deliberate catch-all for tool safety
            return _error_envelope(fn, e)

    return wrapper


# --- Report document fetch + capping ------------------------------------------

def _extract_report_text(doc_payload):
    """Get the report body as text, version-independently.

    The 2021-06-30 Reports API returns an unencrypted (optionally gzip) document
    behind a presigned URL. Some library versions pre-decode it into the payload;
    otherwise we download from ``url`` ourselves — avoiding the ``decrypt=`` vs
    ``download=`` kwarg ambiguity across library versions entirely.
    """
    if not isinstance(doc_payload, dict):
        return str(doc_payload or "")
    if doc_payload.get("document"):
        return doc_payload["document"]
    url = doc_payload.get("url")
    if not url:
        return ""
    raw = None
    for _attempt in range(3):
        try:
            raw = urllib.request.urlopen(url, timeout=30).read()
            break
        except Exception:
            if _attempt == 2:
                raise
            time.sleep(2 ** _attempt)
    if doc_payload.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _cap_report(text, report_type, max_rows, fields, by="date"):
    """Bound a report body for the token budget.

    JSON reports (sales & traffic) are parsed and returned as an object with a
    char guard. TSV/flat-file reports get a row cap and optional column
    projection via ``fields`` (comma-separated header names).
    """
    if report_type in _JSON_REPORTS:
        try:
            doc = json.loads(text)
        except Exception:
            return {"format": "json", "content": text[:_MAX_JSON_CHARS],
                    "truncated": len(text) > _MAX_JSON_CHARS}
        by_asin = doc.get("salesAndTrafficByAsin") or []
        out = {"format": "json", "reportSpecification": doc.get("reportSpecification")}
        if by in ("date", "both"):
            out["salesAndTrafficByDate"] = doc.get("salesAndTrafficByDate") or []
        if by in ("asin", "both"):
            out["salesAndTrafficByAsin"] = by_asin[:max_rows]
            out["asinCount"] = len(by_asin)
            out["asinTruncated"] = len(by_asin) > max_rows
        return out
    lines = text.splitlines()
    if not lines:
        return {"format": "tsv", "rows": 0, "truncated": False, "content": ""}
    header, body = lines[0], lines[1:]
    truncated = len(body) > max_rows
    body = body[:max_rows]
    if fields:
        cols = [c.strip() for c in fields.split(",") if c.strip()]
        head = header.split("\t")
        idx = [head.index(c) for c in cols if c in head]
        def pick(line):
            cells = line.split("\t")
            return "\t".join(cells[i] if i < len(cells) else "" for i in idx)
        out = [pick(header)] + [pick(r) for r in body]
        content = "\n".join(out)
    else:
        content = "\n".join([header] + body)
    return {"format": "tsv", "rows": len(body), "truncated": truncated,
            "content": content}


# --- Health check (so the Docker HEALTHCHECK on /health passes) ----------------

@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


# --- Orders -------------------------------------------------------------------

@mcp.tool()
@tool_safe
def get_orders(days_ago: int = 7, created_after: str = None, created_before: str = None,
               status: str = "Unshipped,PartiallyShipped,Shipped",
               fulfillment_channels: str = None, max_orders: int = 200,
               next_token: str = None, fields: str = None) -> dict:
    """Get orders for the last N days OR an explicit IST date range
    (created_after/created_before, ISO date or datetime). status is a comma list,
    or "all" to omit the filter. fulfillment_channels filters AFN/MFN. Paginated up
    to ~max_orders (page-aligned, ~100/page); pass back next_token to resume. fields
    is a comma-separated projection (e.g. "AmazonOrderId,OrderStatus,OrderTotal,
    PurchaseDate") to shrink the response. PurchaseDate values are UTC.
    A created_before within ~2 min of now is rejected by Amazon; pass an older
    bound or omit it (omitting returns orders up to now)."""
    ca, cb = _window(days_ago, created_after, created_before)
    statuses = None if status.strip().lower() == "all" else [s.strip() for s in status.split(",") if s.strip()]
    channels = [c.strip() for c in fulfillment_channels.split(",") if c.strip()] if fulfillment_channels else None

    def call(token):
        cursor = token or next_token  # resume from a caller-supplied next_token on page 1
        if cursor:
            kwargs = {"NextToken": cursor}  # Amazon rejects a token combined with other filters
        else:
            kwargs = {"CreatedAfter": ca, "MarketplaceIds": [MARKETPLACE]}
            # getOrders rejects a CreatedBefore within ~2 min of now; only send an
            # upper bound when the caller explicitly asked for one. Omitting it lets
            # Amazon default to "up to now" safely.
            if created_before:
                kwargs["CreatedBefore"] = cb
            if statuses:
                kwargs["OrderStatuses"] = statuses
            if channels:
                kwargs["FulfillmentChannels"] = channels
        return Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_orders(**kwargs).payload

    orders, truncated, tok = _paginate(call, "Orders", lambda p: p.get("NextToken"), max_items=max_orders)
    orders = _project(orders, fields)
    return {"orders": orders, "count": len(orders), "truncated": truncated, "next_token": tok}


@mcp.tool()
@tool_safe
def get_order(order_id: str) -> dict:
    """Get full details for a specific order ID."""
    return Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order(order_id).payload


@mcp.tool()
@tool_safe
def get_order_items(order_id: str, max_items: int = 500, fields: str = None) -> dict:
    """Get line items for a specific order (paginated for large orders)."""
    def call(token):
        kwargs = {"NextToken": token} if token else {}
        return Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order_items(order_id, **kwargs).payload
    items, truncated, tok = _paginate(call, "OrderItems", lambda p: p.get("NextToken"), max_items=max_items)
    items = _project(items, fields)
    return {"order_id": order_id, "items": items, "count": len(items), "truncated": truncated, "next_token": tok}


# --- Inventory (FBA) ----------------------------------------------------------

@mcp.tool()
@tool_safe
def get_inventory_summary(seller_skus: str = None, details: bool = True,
                          max_items: int = 500, fields: str = None,
                          next_token: str = None) -> dict:
    """Get FBA inventory summary across all SKUs (paginated). details=True (default)
    includes the fulfillable/inbound/reserved/unfulfillable breakdown; set
    details=False for a lighter list. seller_skus is an optional comma list. fields
    is a comma-separated projection (e.g. "sellerSku,asin,totalQuantity"); pass
    next_token to resume. Paginated up to ~max_items."""
    skus = [s.strip() for s in seller_skus.split(",") if s.strip()] if seller_skus else None

    def call(token):
        # FBA Inventory requires the SAME params on every page plus startDateTime to
        # emit a pagination token at all. FBA Inventory rejects startDateTime older
        # than ~18 months, so use ~540 days (the max safe lookback) — this both
        # enables the pagination token and covers essentially all active inventory.
        kwargs = {"details": details, "granularityType": "Marketplace",
                  "granularityId": MARKETPLACE, "marketplaceIds": [MARKETPLACE],
                  "startDateTime": _iso(datetime.now(timezone.utc) - timedelta(days=540))}
        if skus:
            kwargs["sellerSkus"] = skus
        cursor = token or next_token
        if cursor:
            kwargs["nextToken"] = cursor
        resp = Inventories(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_inventory_summary_marketplace(**kwargs)
        return {**(resp.payload or {}), "pagination": getattr(resp, "pagination", None)}

    items, truncated, tok = _paginate(
        call, "inventorySummaries",
        lambda p: (p.get("pagination") or {}).get("nextToken"), max_items=max_items)
    items = _project(items, fields)
    return {"inventory": items, "count": len(items), "truncated": truncated, "next_token": tok}


# --- Sales metrics ------------------------------------------------------------

@mcp.tool()
@tool_safe
def get_sales_metrics(days_ago: int = 30, granularity: str = "Day",
                      created_after: str = None, created_before: str = None) -> dict:
    """Get sales aggregates bucketed by granularity (Day | Week | Month | ...).
    Window is the last N days OR an explicit IST date range; it is floored to IST
    midnight so buckets are whole days (no ragged partial edges). Returns ordered
    units, order count, average selling price, average units per order."""
    try:
        gran = Granularity[granularity.upper()]
    except KeyError:
        valid = ", ".join(g.name.title() for g in Granularity)
        return {"error": {"type": "ValueError",
                          "message": f"Invalid granularity {granularity!r}; use one of: {valid}"}}
    start, end = _window(days_ago, created_after, created_before, floor_to_day=True)
    res = Sales(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order_metrics(
        interval=(start, end),
        granularity=gran,
        granularityTimeZone="Asia/Kolkata",
        marketplaceIds=[MARKETPLACE],
    )
    # get_order_metrics returns a JSON array; MCP structured output needs a
    # top-level object, so wrap the list under a key.
    return {"metrics": res.payload if isinstance(res.payload, list) else []}


# --- Reports (async — request → poll → download) ------------------------------

def _poll_report(r, report_id, *, attempts=10, delay=4):
    """Poll a report to completion (status checked before the first sleep).

    Bounded to ~attempts*delay seconds to stay under the MCP gateway timeout.
    Returns the document descriptor on DONE, or an error/in-progress envelope
    (always carrying reportId so a caller can re-fetch with report_id=...).
    """
    for _ in range(attempts):
        status = (_with_retry(lambda: r.get_report(report_id), op="getReport").payload) or {}
        ps = status.get("processingStatus")
        if ps == "DONE":
            doc_id = status.get("reportDocumentId")
            if not doc_id:
                return {"error": {"type": "ReportError",
                                  "message": "report DONE but no reportDocumentId",
                                  "reportId": report_id}}
            return (_with_retry(lambda: r.get_report_document(doc_id),
                                op="getReportDocument").payload) or {}
        if ps == "CANCELLED":
            # Amazon cancels reports with no records for the period — treat as empty, not an error.
            return {"status": "NO_DATA", "reportId": report_id,
                    "message": "report cancelled by Amazon — usually means no records for the period"}
        if ps == "FATAL":
            return {"error": {"type": "ReportError", "message": "report FATAL",
                              "reportId": report_id}}
        time.sleep(delay)
    return {"status": "IN_PROGRESS", "reportId": report_id,
            "message": "report still processing; re-call this tool with "
                       f"report_id={report_id!r} to fetch it once ready"}


def _request_and_fetch_report(report_type, days_ago=30, after=None, before=None,
                              extra_options=None, max_rows=500, fields=None,
                              by="date", report_id=None):
    """Request a report (or resume an existing one), poll, fetch, and bound it.

    Pass ``report_id`` to skip creation and just fetch an already-requested
    report — the lightweight retry path for reports that outlive the gateway
    timeout. Dict access is guarded; non-SP errors no longer escape.
    """
    r = Reports(credentials=creds(), marketplace=MARKETPLACE_ENUM)
    if not report_id:
        start, end = _window(days_ago, after, before)
        opts = {"reportType": report_type, "marketplaceIds": [MARKETPLACE],
                "dataStartTime": start, "dataEndTime": end}
        if extra_options:
            opts.update(extra_options)
        created = (_with_retry(lambda: r.create_report(**opts), op="createReport").payload) or {}
        report_id = created.get("reportId")
        if not report_id:
            return {"error": {"type": "ReportError",
                              "message": "create_report returned no reportId",
                              "payload": created}}
    doc = _poll_report(r, report_id)
    if "error" in doc or "status" in doc:
        return doc
    result = _cap_report(_extract_report_text(doc), report_type, max_rows, fields, by=by)
    result["reportId"] = report_id
    return result


def _latest_settlement_report(max_rows=500, fields=None):
    """Settlement reports are scheduled by Amazon and can't be requested via
    create_report — list the available ones and fetch the most recent DONE doc.
    """
    r = Reports(credentials=creds(), marketplace=MARKETPLACE_ENUM)
    listing = (_with_retry(lambda: r.get_reports(
        reportTypes=["GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"]), op="getReports").payload) or {}
    done = [x for x in listing.get("reports", [])
            if x.get("processingStatus") == "DONE" and x.get("reportDocumentId")]
    if not done:
        return {"error": {"type": "ReportError",
                          "message": "no settlement reports available yet"}}
    latest = max(done, key=lambda x: x.get("dataEndTime", ""))
    doc = (_with_retry(lambda: r.get_report_document(
        latest["reportDocumentId"]), op="getReportDocument").payload) or {}
    result = _cap_report(_extract_report_text(doc),
                         "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE", max_rows, fields)
    result["reportId"] = latest.get("reportId")
    result["dataEndTime"] = latest.get("dataEndTime")
    return result


# Report tools return a bounded envelope: TSV reports → {rows, truncated, content}
# capped at max_rows with optional column projection via fields; the JSON report
# (sales & traffic) → {content: <object>}. Long reports can exceed the MCP gateway
# timeout — when that happens the tool returns {status:"IN_PROGRESS", reportId}; the
# report keeps generating server-side, so re-call the same tool with report_id=<id>
# to fetch it once ready.

@mcp.tool()
@tool_safe
def get_fba_reimbursements(days_ago: int = 60, after: str = None, before: str = None,
                           max_rows: int = 500, fields: str = None, report_id: str = None) -> dict:
    """Get FBA reimbursements for lost or damaged inventory (last N days or IST range)."""
    return _request_and_fetch_report("GET_FBA_REIMBURSEMENTS_DATA", days_ago, after, before,
                                     max_rows=max_rows, fields=fields, report_id=report_id)


@mcp.tool()
@tool_safe
def get_settlement_report(max_rows: int = 500, fields: str = None) -> dict:
    """Get the most recent settlement/disbursement report. Settlement reports are
    scheduled by Amazon (not requestable on demand), so this returns the latest
    available one. fields projects TSV columns; max_rows caps rows."""
    return _latest_settlement_report(max_rows=max_rows, fields=fields)


@mcp.tool()
@tool_safe
def get_fba_storage_fees(days_ago: int = 30, after: str = None, before: str = None,
                         max_rows: int = 500, fields: str = None, report_id: str = None) -> dict:
    """Get FBA monthly storage fee charges by SKU/ASIN (last N days or IST range)."""
    return _request_and_fetch_report("GET_FBA_STORAGE_FEE_CHARGES_DATA", days_ago, after, before,
                                     max_rows=max_rows, fields=fields, report_id=report_id)


@mcp.tool()
@tool_safe
def get_fba_long_term_storage_fees(max_rows: int = 500, fields: str = None,
                                   report_id: str = None) -> dict:
    """Get FBA long-term storage fees (inventory aged 180+ and 365+ days)."""
    return _request_and_fetch_report("GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA",
                                     days_ago=180, max_rows=max_rows, fields=fields, report_id=report_id)


@mcp.tool()
@tool_safe
def get_inventory_ledger(days_ago: int = 30, after: str = None, before: str = None,
                         aggregated_by: str = "DAILY", aggregate_by_location: str = "FC",
                         max_rows: int = 500, fields: str = None, report_id: str = None) -> dict:
    """Get inventory ledger report — inventory movements (receipts, shipments,
    removals, adjustments) for the last N days or an IST date range. The SUMMARY
    view aggregates by whole periods: aggregated_by="DAILY" (default) returns one
    row per SKU per day so a rolling window yields data; "MONTHLY" returns one row
    per SKU per complete calendar month. aggregate_by_location="FC" (default) keeps
    per-fulfillment-center rows; "COUNTRY" collapses them to one row per SKU/period.
    A 30-day DAILY/FC ledger is large — narrow the range or use fields/max_rows."""
    extra = None if report_id else {
        "reportOptions": {"aggregateByLocation": aggregate_by_location,
                          "aggregatedByTimePeriod": aggregated_by}}
    return _request_and_fetch_report("GET_LEDGER_SUMMARY_VIEW_DATA", days_ago, after, before,
                                     extra_options=extra, max_rows=max_rows, fields=fields,
                                     report_id=report_id)


@mcp.tool()
@tool_safe
def get_fulfilled_shipments(days_ago: int = 30, after: str = None, before: str = None,
                            max_rows: int = 500, fields: str = None,
                            report_id: str = None) -> dict:
    """Get the FBA Amazon-Fulfilled Shipments report — one row per shipped item
    with seller SKU, quantity shipped, and the customer ship-to city / state /
    postal code. This is the source for regional demand analysis: which pincodes
    drive which SKUs. Last N days or an IST date range; Amazon caps the window at
    roughly 30 days. The report is large — bound it with max_rows/fields."""
    return _request_and_fetch_report("GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL",
                                     days_ago, after, before,
                                     max_rows=max_rows, fields=fields,
                                     report_id=report_id)


@mcp.tool()
@tool_safe
def get_report(report_type: str, days_ago: int = 30, after: str = None, before: str = None,
               max_rows: int = 500, fields: str = None, report_options: dict = None,
               report_id: str = None) -> dict:
    """Request ANY SP-API report by its report type string, poll, and return it.

    Escape hatch for report types that have no dedicated tool, e.g.
    GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA (per-SKU inbound working/shipped/
    receiving quantities) or GET_MERCHANT_LISTINGS_ALL_DATA. Pass report_options
    as a dict when the report needs them. Returns {"status": "IN_PROGRESS",
    "reportId": ...} while Amazon generates it — re-call with report_id to fetch."""
    return _request_and_fetch_report(report_type, days_ago, after, before,
                                     extra_options=({"reportOptions": report_options}
                                                    if report_options and not report_id else None),
                                     max_rows=max_rows, fields=fields, report_id=report_id)


# --- Inbound walks (Fulfillment Inbound v2024-03-20) --------------------------
# Read-only. The walks retry 5xx up to 7 attempts (a planner reading every shipment
# has no use for a partial answer); 429s are paced by the buckets and retried on
# their own budget. Per-plan and per-shipment reads run concurrently in threads
# (the SP-API client is sync), results reassembled in listing order.

_PATIENT = 7
_PLAN_FANOUT = 6  # plans read at once: the getInboundPlan burst
_POOL = ThreadPoolExecutor(max_workers=32, thread_name_prefix="sp-api")
IN_TRANSIT_STATUSES = "READY_TO_SHIP,SHIPPED,IN_TRANSIT,DELIVERED,CHECKED_IN,RECEIVING"
_TERMINAL = {"CLOSED", "CANCELLED", "DELETED"}
# Shipments in a terminal state never change, so each is read once per process:
# getShipment payloads keyed (plan_id, shipment_id), selected transportation options
# keyed (plan_id, shipment_id, option_id). In memory only: a restart (every deploy)
# starts empty and the next walk re-reads them, so correctness never depends on it.
_SHIPMENT_CACHE: dict = {}
_TRANSPORT_CACHE: dict = {}


def _inbound():
    """A fresh v2024 client: sp_api clients keep per-request state, so threads never share one."""
    from sp_api.api import FulfillmentInboundV20240320 as G
    return G(credentials=creds(), marketplace=MARKETPLACE_ENUM)


async def _io(fn, *args):
    """Run one blocking SP-API read in the walk pool."""
    return await asyncio.get_running_loop().run_in_executor(_POOL, partial(fn, *args))


def _ts(v):
    """An ISO timestamp as an aware datetime, or None."""
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def _list_page(kw, attempts):
    return _with_retry(lambda: _inbound().list_inbound_plans(**kw),
                       max_attempts=attempts, op="listInboundPlans")


def _get_plan(pid, attempts):
    return (_with_retry(lambda: _inbound().get_inbound_plan(inboundPlanId=pid),
                        max_attempts=attempts, op="getInboundPlan").payload) or {}


def _get_shipment(pid, sid, attempts):
    """getShipment's payload; a terminal shipment's comes from the cache after its first read."""
    hit = _SHIPMENT_CACHE.get((pid, sid))
    if hit is not None:
        return hit
    full = (_with_retry(lambda: _inbound().get_shipment(inboundPlanId=pid, shipmentId=sid),
                        max_attempts=attempts, op="getShipment").payload) or {}
    if (full.get("status") or "").upper() in _TERMINAL:
        _SHIPMENT_CACHE[(pid, sid)] = full
    return full


async def _walk(statuses, cutoff, read, *, max_plans=None, attempts=_PATIENT):
    """List plans (each status in order, every page, newest update first) and read each.

    A plan listed twice is read once; one created before ``cutoff`` is left out. Paging
    stops after a page holding a plan last updated before ``cutoff``: the listing is
    sorted by last update, and a plan is never updated before it was created, so every
    later plan is outside the window too. If the update times seen so far are ever out
    of order, that status is paged to the end instead. ``read(pid, created_at)`` runs concurrently
    (at most _PLAN_FANOUT at once) while the listing continues; returns (results in
    listing order, pages). ``statuses`` of [None] lists without a status filter.
    """
    sem = asyncio.Semaphore(_PLAN_FANOUT)

    async def bounded(pid, created):
        async with sem:
            return await read(pid, created)

    tasks, seen, pages = [], set(), 0
    try:
        for status in statuses:
            token, tokens, last, ordered = None, set(), None, True
            while True:
                kw = {"pageSize": 30}
                if status:
                    kw["status"] = status
                if token:
                    kw["paginationToken"] = token
                else:  # sort on the first page only, as the pickups listing does
                    kw.update(sortBy="LAST_UPDATED_TIME", sortOrder="DESC")
                resp = await _io(_list_page, kw, attempts)
                pages += 1
                stale = full = False
                for p in (resp.payload or {}).get("inboundPlans") or []:
                    updated = _ts(p.get("lastUpdatedAt"))
                    if updated:
                        # Stop early only while the listing is seen to be sorted.
                        ordered = ordered and (last is None or updated <= last)
                        last = updated
                        stale = stale or updated < cutoff
                    pid, created = p.get("inboundPlanId"), p.get("createdAt")
                    if not pid or pid in seen:
                        continue
                    born = _ts(created) if created else None
                    if born and born < cutoff:
                        continue
                    if max_plans is not None and len(seen) >= max_plans:
                        full = True
                        break
                    seen.add(pid)
                    tasks.append(asyncio.ensure_future(bounded(pid, created)))
                # The library hoists `pagination` off .payload; next_token reads both.
                token = resp.next_token
                if not token or full or (stale and ordered):
                    break
                if token in tokens:
                    raise RuntimeError(f"pagination token repeated for status {status}")
                tokens.add(token)
        return list(await asyncio.gather(*tasks)), pages
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def _shipment_items(plan_id, shipment_id, confirmation_id=None):
    """{"items": [{msku, shipped, received}], "source"} (see get_inbound_shipment_items)."""
    err = "no confirmation id"
    if confirmation_id:
        from sp_api.api import FulfillmentInbound as V0
        v0 = V0(credentials=creds(), marketplace=MARKETPLACE_ENUM)
        try:
            data = (_with_retry(lambda: v0.shipment_items_by_shipment(confirmation_id),
                                max_attempts=_PATIENT,
                                op="getShipmentItemsByShipmentId").payload) or {}
            return {"items": [{"msku": i["SellerSKU"], "shipped": int(i["QuantityShipped"]),
                               "received": int(i["QuantityReceived"])}
                              for i in data.get("ItemData") or []],
                    "source": "v0"}
        except Exception as e:  # noqa: BLE001 - any v0 failure falls back to v2024
            err = f"{type(e).__name__}: {e}"[:300]
    g = _inbound()
    items, token, tokens = [], None, set()
    while True:
        kw = {"inboundPlanId": plan_id, "shipmentId": shipment_id}
        if token:
            kw["paginationToken"] = token
        resp = _with_retry(lambda: g.list_shipment_items(**kw), max_attempts=_PATIENT,
                           op="listShipmentItems")
        items += [{"msku": i["msku"], "shipped": int(i["quantity"]), "received": 0}
                  for i in (resp.payload or {}).get("items") or [] if i.get("msku")]
        token = resp.next_token
        if not token:
            break
        if token in tokens:
            raise RuntimeError("pagination token repeated")
        tokens.add(token)
    return {"items": items, "source": "v2024", "v0_error": err}


@mcp.tool()
@tool_safe
async def get_inbound_by_fc(statuses: str = IN_TRANSIT_STATUSES, max_plans: int = 200) -> dict:
    """Inbound units still in transit, broken down by DESTINATION fulfilment centre and SKU.

    Walks FBA inbound plans (Fulfillment Inbound v2024-03-20) listed as ACTIVE and
    SHIPPED (a listing with no status returns only ACTIVE, which misses un-received
    units of SHIPPED plans) and created in the last 120 days, reads each shipment's
    status and destination.warehouseId, and for shipments whose status is in ``statuses`` (default: transport confirmed, so
    WORKING is left out) sums shipped - received per item, floored at 0. Received
    comes from Fulfillment Inbound v0 (as get_inbound_shipment_items; without a
    confirmation id it is 0). These are fba-planner ship_agent/inbound.py's rules.
    FC codes are raw (a DED3 shipment is DED3) and the planner's scratch drafts are
    not known here. The per-SKU inbound totals in the MYI report are national and
    cannot say which FC the stock is heading to.

    Returns {"rows": [{fc, sku, qty}], "plans": n, "shipments": n,
    "skipped_awd_plans": n, "by_status": {status: units}, "total": n}. AWD (Amazon
    Warehousing and Distribution) plans can't be read by this API and are skipped.
    """
    want = {s.strip().upper() for s in statuses.split(",") if s.strip()}
    cutoff = datetime.now(timezone.utc) - timedelta(days=120)

    async def shipment(pid, sh):
        sid, st = sh.get("shipmentId"), (sh.get("status") or "").upper()
        if not sid:
            return None
        if st not in want and st in _TERMINAL:
            return st, None, None  # listed terminal: it never changes, nothing to read
        full = await _io(_get_shipment, pid, sid, _PATIENT)
        st = (full.get("status") or st).upper()
        if st not in want:
            return st, None, None
        fc = ((full.get("destination") or {}).get("warehouseId")) or "UNKNOWN"
        got = await _io(_shipment_items, pid, sid, full.get("shipmentConfirmationId"))
        return st, fc, got["items"]

    async def read(pid, _created):
        try:
            det = await _io(_get_plan, pid, _PATIENT)
        except SellingApiException as e:
            # AWD inbound plans appear in the list but can't be fetched by this API.
            if "Amazon Warehousing and Distribution" not in str(e):
                raise
            return None
        return await asyncio.gather(*(shipment(pid, sh) for sh in det.get("shipments") or []))

    plans, _pages = await _walk(["ACTIVE", "SHIPPED"], cutoff, read, max_plans=max_plans)
    agg, by_status, nship = {}, {}, 0
    for ships in plans:
        for rec in ships or []:
            if rec is None:
                continue
            st, fc, items = rec
            by_status[st] = by_status.get(st, 0)
            if items is None:
                continue
            nship += 1
            for it in items:
                n = max(0, it["shipped"] - it["received"])
                if n:
                    agg[(fc, it["msku"])] = agg.get((fc, it["msku"]), 0) + n
                    by_status[st] += n

    rows = [{"fc": fc, "sku": sku, "qty": q} for (fc, sku), q in sorted(agg.items())]
    return {"rows": rows, "plans": len(plans), "shipments": nship,
            "skipped_awd_plans": sum(1 for p in plans if p is None),
            "by_status": by_status, "total": sum(r["qty"] for r in rows)}


def _selected_option(pid, sid, sel, terminal):
    """The shipment's selected transportation option ({} if not listed); cached when terminal."""
    hit = _TRANSPORT_CACHE.get((pid, sid, sel))
    if hit is not None:
        return hit
    g = _inbound()
    opt, otok, op = {}, None, 0
    while op < 10 and not opt:
        op += 1
        okw = {"inboundPlanId": pid, "shipmentId": sid}
        if otok:
            okw["paginationToken"] = otok
        r = _with_retry(lambda: g.list_transportation_options(**okw),
                        op="listTransportationOptions")
        for o in ((r.payload or {}).get("transportationOptions") or []):
            if o.get("transportationOptionId") == sel:
                opt = o
                break
        otok = r.next_token
        if not otok:
            break
    if terminal:
        _TRANSPORT_CACHE[(pid, sid, sel)] = opt
    return opt


@mcp.tool()
@tool_safe
async def get_confirmed_inbound_pickups(fcs: str = "", statuses: str = "READY_TO_SHIP",
                                        days: int = 30) -> dict:
    """Carrier pickup details for confirmed inbound shipments (read-only).

    Walks FBA inbound plans (Fulfillment Inbound v2024-03-20, newest first,
    stopping once plans are older than ``days``), keeps shipments whose status is
    in ``statuses`` and whose destination FC is in ``fcs`` (comma-separated FC
    codes; empty means every FC), and returns the
    selected transportation option: carrier, shipping solution, pickup
    appointment window, voidable-until and cost.

    Returns {"shipments": [{plan_id, shipment_id, confirmation_id, status, fc,
    transportation_option_id, carrier, shipping_solution, appt_start, appt_end,
    voidable_until, cost_inr}], "plans_scanned", "pages", "skipped"}. ``skipped``
    counts plans that could not be read (AWD plans).
    """
    want_st = {s.strip().upper() for s in statuses.split(",") if s.strip()}
    want_fc = {s.strip().upper() for s in fcs.split(",") if s.strip()}
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    async def shipment(pid, sh):
        sid = sh.get("shipmentId")
        if not sid:
            return None
        st = (sh.get("status") or "").upper()
        if st and st not in want_st:
            return None
        full = await _io(_get_shipment, pid, sid, 5)
        if not st:
            st = (full.get("status") or "").upper()
            if st not in want_st:
                return None
        fc = ((full.get("destination") or {}).get("warehouseId")) or ""
        if want_fc and fc.upper() not in want_fc:
            return None
        sel = full.get("selectedTransportationOptionId")
        terminal = (full.get("status") or "").upper() in _TERMINAL
        opt = await _io(_selected_option, pid, sid, sel, terminal)
        ap = opt.get("carrierAppointment") or {}
        qt = opt.get("quote") or {}
        return {
            "plan_id": pid, "shipment_id": sid,
            "confirmation_id": full.get("shipmentConfirmationId"),
            "status": st, "fc": fc, "transportation_option_id": sel,
            "carrier": (opt.get("carrier") or {}).get("alphaCode"),
            "shipping_solution": opt.get("shippingSolution"),
            "appt_start": ap.get("startTime"), "appt_end": ap.get("endTime"),
            "voidable_until": qt.get("voidableUntil"),
            "cost_inr": (qt.get("cost") or {}).get("amount"),
        }

    sem = asyncio.Semaphore(_PLAN_FANOUT)

    async def read(pid):
        async with sem:
            try:
                det = await _io(_get_plan, pid, 5)
            except SellingApiException as e:
                if getattr(e, "code", None) == 400:
                    return None
                raise
            return await asyncio.gather(*(shipment(pid, sh) for sh in det.get("shipments") or []))

    tasks, pages, token, done = [], 0, None, False
    try:
        while pages < 20 and not done:
            pages += 1
            kw = {"pageSize": 30}
            if token:
                kw["paginationToken"] = token
            else:
                kw.update(status="ACTIVE", sortBy="CREATION_TIME", sortOrder="DESC")
            resp = await _io(_list_page, kw, 5)
            for p in ((resp.payload or {}).get("inboundPlans") or []):
                created = _ts(p.get("createdAt"))
                if created and created < cutoff:
                    done = True
                    break
                pid = p.get("inboundPlanId")
                if not pid:
                    continue
                tasks.append(asyncio.ensure_future(read(pid)))
            token = resp.next_token
            if not token:
                break
        plans = await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    out = [row for ships in plans if ships is not None for row in ships if row]
    return {"shipments": out, "plans_scanned": sum(1 for p in plans if p is not None),
            "pages": pages, "skipped": sum(1 for p in plans if p is None)}


@mcp.tool()
@tool_safe
def get_sales_traffic_report(days_ago: int = 30, after: str = None, before: str = None,
                             by: str = "date", max_rows: int = 500, report_id: str = None) -> dict:
    """Get sales & traffic report (JSON). by="date" returns daily KPIs (sessions,
    page views, units, sales, buy-box %, conversion) — small & complete; by="asin"
    returns per-ASIN rows (capped at max_rows); by="both" returns both. Last N days
    or an IST range."""
    return _request_and_fetch_report("GET_SALES_AND_TRAFFIC_REPORT", days_ago, after, before,
                                     max_rows=max_rows, by=by, report_id=report_id)


# --- Finances -----------------------------------------------------------------

@mcp.tool()
@tool_safe
def get_financial_events(days_ago: int = 30, after: str = None, before: str = None,
                         event_types: str = None, max_events: int = 500,
                         next_token: str = None) -> dict:
    """Get per-item charges and fees for the last N days or an IST date range,
    flattened to one row per shipped/refunded item: posted_date, order_id, sku,
    quantity, principal, tax, a fee_type -> amount dict and total_fees. Amounts use
    Amazon's signs (fees negative; refund rows negative throughout). event_types is
    a comma list of "shipment" and/or "refund" (default both). Paginated up to
    ~max_events rows; pass back next_token to resume."""
    start, end = _window(days_ago, after, before)
    lists = {"shipment": "ShipmentEventList", "refund": "RefundEventList"}
    wanted = [e.strip().lower() for e in event_types.split(",")] if event_types else list(lists)
    rows, token = [], next_token
    for _ in range(20):
        # Amazon rejects a NextToken combined with the date filters, so send one or the other.
        kwargs = {"NextToken": token} if token else {"PostedAfter": start, "PostedBefore": end,
                                                    "MaxResultsPerPage": 100}
        payload = _with_retry(lambda: FinancesV0(credentials=creds(), marketplace=MARKETPLACE_ENUM)
                              .list_financial_events(**kwargs).payload) or {}
        events = payload.get("FinancialEvents") or {}
        for kind in wanted:
            for ev in (events.get(lists.get(kind)) or []):
                # Refund events carry the same shape under *Adjustment* keys.
                for it in (ev.get("ShipmentItemList") or ev.get("ShipmentItemAdjustmentList") or []):
                    charges = {c.get("ChargeType"): (c.get("ChargeAmount") or {}).get("CurrencyAmount", 0)
                               for c in (it.get("ItemChargeList") or it.get("ItemChargeAdjustmentList") or [])}
                    fees = {}
                    for f in (it.get("ItemFeeList") or it.get("ItemFeeAdjustmentList") or []):
                        amt = (f.get("FeeAmount") or {}).get("CurrencyAmount", 0)
                        fees[f.get("FeeType")] = round(fees.get(f.get("FeeType"), 0) + amt, 2)
                    rows.append({"posted_date": ev.get("PostedDate"), "order_id": ev.get("AmazonOrderId"),
                                 "sku": it.get("SellerSKU"), "quantity": it.get("QuantityShipped"),
                                 "event_type": kind, "principal": charges.get("Principal", 0),
                                 "tax": round(sum(v for k, v in charges.items() if k and k.endswith("Tax")), 2),
                                 "fees": fees, "total_fees": round(sum(fees.values()), 2)})
        token = payload.get("NextToken")
        if not token or len(rows) >= max_events:
            break
    return {"events": rows[:max_events], "count": min(len(rows), max_events),
            "truncated": len(rows) > max_events, "next_token": token}


# --- Fee estimates ------------------------------------------------------------

@mcp.tool()
@tool_safe
def get_fee_estimate(asin: str, price: float, is_fba: bool = True,
                     currency: str = "INR") -> dict:
    """Get a fee estimate for an ASIN at a given selling price.
    Returns referral fee, fulfillment fee (FBA when is_fba), total estimated fees."""
    return ProductFees(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_product_fees_estimate_for_asin(
        asin, price, currency=currency, is_fba=is_fba
    ).payload


# --- Catalog / listings -------------------------------------------------------

@mcp.tool()
@tool_safe
def get_catalog_item(asin: str, included_data: str = "attributes,summaries,images",
                     fields: str = None) -> dict:
    """Get product details for an ASIN — title, brand, dimensions, images.
    included_data is a comma list (the library's CatalogItems uses API 2020-12-01,
    where dimensions live inside "attributes"). fields projects top-level keys of
    the response to trim the (heavy) attributes block."""
    payload = CatalogItems(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_catalog_item(
        asin=asin,
        marketplaceIds=[MARKETPLACE],
        includedData=[s.strip() for s in included_data.split(",") if s.strip()],
    ).payload
    if fields:
        keys = [f.strip() for f in fields.split(",") if f.strip()]
        return {k: payload[k] for k in keys if k in payload}
    return payload


@mcp.tool()
@tool_safe
def search_catalog(keywords: str, page_size: int = 10, page_token: str = None,
                   included_data: str = "summaries") -> dict:
    """Search catalog by keywords. Returns ASINs, titles, and basic product info.
    Pass page_token (from a previous response's next_token) to fetch the next page.
    included_data is a comma list."""
    included = [s.strip() for s in included_data.split(",") if s.strip()]
    kwargs = {"keywords": keywords, "marketplaceIds": [MARKETPLACE],
              "includedData": included, "pageSize": page_size}
    if page_token:
        kwargs["pageToken"] = page_token
    resp = CatalogItems(credentials=creds(), marketplace=MARKETPLACE_ENUM).search_catalog_items(**kwargs)
    payload = resp.payload or {}
    payload["next_token"] = (getattr(resp, "pagination", None) or {}).get("nextToken")
    return payload


# --- Inbound reads, pricing, listings ----------------------------------------


async def _inbound_plan(pid, created_at=None):
    """One plan with each shipment's status, confirmation id and destination FC.

    A shipment's status is getShipment's, else the one listed on the plan."""
    det = await _io(_get_plan, pid, _PATIENT)
    listed = [sh for sh in det.get("shipments") or [] if sh.get("shipmentId")]
    fulls = await asyncio.gather(*(_io(_get_shipment, pid, sh["shipmentId"], _PATIENT)
                                   for sh in listed))
    ships = [{"shipment_id": sh["shipmentId"], "status": full.get("status") or sh.get("status"),
              "confirmation_id": full.get("shipmentConfirmationId"),
              "destination": (full.get("destination") or {}).get("warehouseId")}
             for sh, full in zip(listed, fulls)]
    return {"plan_id": pid, "status": det.get("status"),
            "created_at": created_at or det.get("createdAt"), "shipments": ships}


@mcp.tool()
@tool_safe
async def get_inbound_plans(statuses: str = "ACTIVE,SHIPPED", days: int = 120,
                            plan_ids: str = None) -> dict:
    """Inbound plans (Fulfillment Inbound v2024-03-20) with every shipment's status,
    confirmation id (FBA...) and destination FC (read-only).

    Lists plans for each status in ``statuses`` (comma-separated, in that order),
    newest update first, following every page until plans fall outside the window,
    and keeps plans created in the last ``days`` days; a plan listed under two
    statuses appears once. Or pass ``plan_ids`` (comma-separated) to read just those
    plans, with no listing. AWD plans (GetInboundPlan answers 400) are skipped and
    counted. Never partial: any other failure is an error.

    Returns {"plans": [{plan_id, status, created_at, shipments: [{shipment_id,
    status, confirmation_id, destination}]}], "pages", "skipped"}.
    """
    if plan_ids:
        ids = [x.strip() for x in plan_ids.split(",") if x.strip()]
        sem = asyncio.Semaphore(_PLAN_FANOUT)

        async def one(pid):
            async with sem:
                return await _inbound_plan(pid)

        return {"plans": list(await asyncio.gather(*(one(pid) for pid in ids))),
                "pages": 0, "skipped": 0}

    async def read(pid, created):
        try:
            return await _inbound_plan(pid, created)
        except SellingApiException as e:
            if getattr(e, "code", None) != 400:
                raise
            return None  # AWD plan: GetInboundPlan does not support it

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    found, pages = await _walk([x.strip().upper() for x in statuses.split(",") if x.strip()],
                               cutoff, read)
    plans = [p for p in found if p is not None]
    return {"plans": plans, "pages": pages, "skipped": len(found) - len(plans)}


@mcp.tool()
@tool_safe
def get_inbound_shipment_items(plan_id: str, shipment_id: str,
                               confirmation_id: str = None) -> dict:
    """Items of one inbound shipment with shipped AND received quantities (read-only).

    v2024-03-20 has only the shipped quantity, so items come from Fulfillment
    Inbound v0 getShipmentItemsByShipmentId, keyed by the shipment confirmation id
    (FBA...; see get_inbound_plans). With no confirmation id, or when the v0 read
    fails, it falls back to v2024 listShipmentItems (every page) with received 0.

    Returns {"items": [{msku, shipped, received}], "source": "v0" | "v2024"} and,
    after a fallback, "v0_error".
    """
    return _shipment_items(plan_id, shipment_id, confirmation_id)


@mcp.tool()
@tool_safe
def get_product_pricing_for_skus(skus: str) -> dict:
    """Your own offer's price for up to 20 seller SKUs (Product Pricing v0
    getPricing, ItemType=Sku; read-only). ``skus`` is comma-separated.

    Returns {"pricing": [...]}: one entry per SKU with ``status`` and, on
    "Success", ``Product.Offers[].BuyingPrice`` (ListingPrice, LandedPrice).
    """
    from sp_api.api import Products
    lst = [x.strip() for x in skus.split(",") if x.strip()]
    if not 1 <= len(lst) <= 20:
        raise ValueError(f"pass 1 to 20 SKUs, got {len(lst)}")
    resp = _with_retry(lambda: Products(credentials=creds(), marketplace=MARKETPLACE_ENUM)
                       .get_product_pricing_for_skus(lst), max_attempts=_PATIENT, op="getPricing")
    return {"pricing": resp.payload if isinstance(resp.payload, list) else []}


@mcp.tool()
@tool_safe
def get_listings_item(seller_id: str, sku: str, included_data: str = "summaries,offers") -> dict:
    """One of your listings by seller SKU (Listings Items 2021-08-01; read-only).

    ``seller_id`` is your merchant token. ``included_data`` is a comma list of
    summaries, attributes, issues, offers, fulfillmentAvailability, procurement.
    Returns the listing payload as Amazon sends it.
    """
    from sp_api.api import ListingsItems
    inc = ",".join(x.strip() for x in included_data.split(",") if x.strip())
    resp = _with_retry(lambda: ListingsItems(credentials=creds(), marketplace=MARKETPLACE_ENUM)
                       .get_listings_item(sellerId=seller_id, sku=sku,
                                          marketplaceIds=[MARKETPLACE], includedData=inc),
                       max_attempts=_PATIENT)
    return resp.payload or {}


# --- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
    )
