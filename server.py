import gzip
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from functools import wraps
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

def _with_retry(call, *, max_attempts=5, base=1.0):
    """Invoke an SP-API thunk, retrying throttle/5xx/transient errors with backoff.

    The library has no built-in retry. We retry on HTTP 429/5xx (read off the
    exception's ``code``) and transient transport/JSON errors; everything else
    (4xx, bad input) is re-raised immediately.
    """
    for attempt in range(max_attempts):
        try:
            return call()
        except Exception as e:
            last = attempt == max_attempts - 1
            if isinstance(e, SellingApiException):
                retryable = getattr(e, "code", None) in _RETRY_CODES
            elif isinstance(e, _RETRY_NETWORK):
                retryable = True
            else:
                retryable = False
            if not retryable or last:
                raise
            time.sleep(base * (2 ** attempt))


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


def tool_safe(fn):
    """Wrap a tool so any exception becomes a structured error envelope.

    Catches everything (not just SellingApiException, so KeyError/httpx/JSON no
    longer escape as raw 500s) and logs unexpected failures server-side so real
    bugs aren't silently swallowed. Keep ``@mcp.tool()`` above this decorator.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except SellingApiException as e:
            return {"error": {"type": type(e).__name__,
                              "code": getattr(e, "code", None), "message": str(e)}}
        except Exception as e:  # noqa: BLE001 - deliberate catch-all for tool safety
            log.exception("tool %s failed", fn.__name__)
            return {"error": {"type": type(e).__name__, "message": str(e)}}

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
        status = (_with_retry(lambda: r.get_report(report_id)).payload) or {}
        ps = status.get("processingStatus")
        if ps == "DONE":
            doc_id = status.get("reportDocumentId")
            if not doc_id:
                return {"error": {"type": "ReportError",
                                  "message": "report DONE but no reportDocumentId",
                                  "reportId": report_id}}
            return (_with_retry(lambda: r.get_report_document(doc_id)).payload) or {}
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
        created = (_with_retry(lambda: r.create_report(**opts)).payload) or {}
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
        reportTypes=["GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"])).payload) or {}
    done = [x for x in listing.get("reports", [])
            if x.get("processingStatus") == "DONE" and x.get("reportDocumentId")]
    if not done:
        return {"error": {"type": "ReportError",
                          "message": "no settlement reports available yet"}}
    latest = max(done, key=lambda x: x.get("dataEndTime", ""))
    doc = (_with_retry(lambda: r.get_report_document(
        latest["reportDocumentId"])).payload) or {}
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


@mcp.tool()
@tool_safe
def get_inbound_by_fc(statuses: str = "WORKING,READY_TO_SHIP,SHIPPED,IN_TRANSIT,DELIVERED,CHECKED_IN,RECEIVING",
                      max_plans: int = 200) -> dict:
    """Inbound units broken down by DESTINATION fulfilment centre and SKU.

    Walks FBA inbound plans (Fulfillment Inbound v2024-03-20), reads each
    shipment's destination.warehouseId, and sums its line items. This is what
    replenishment planning needs: the per-SKU inbound totals in the MYI report
    are national and cannot say which FC the stock is heading to.

    Returns {"rows": [{fc, sku, qty, status}], "plans": n, "shipments": n,
    "skipped_awd_plans": n, "by_status": {...}, "total": n}. AWD (Amazon
    Warehousing and Distribution) plans can't be read by this API and are skipped.
    """
    from sp_api.api import FulfillmentInboundV20240320 as G
    g = G(credentials=creds(), marketplace=MARKETPLACE_ENUM)
    want = {s.strip().upper() for s in statuses.split(",") if s.strip()}

    plans, token, pages = [], None, 0
    while pages < 20:
        pages += 1
        kw = {"pageSize": 30}
        if token:
            kw["paginationToken"] = token
        resp = _with_retry(lambda: g.list_inbound_plans(**kw))
        plans.extend((resp.payload or {}).get("inboundPlans") or [])
        # The library hoists `pagination` off .payload onto the response object.
        token = (getattr(resp, "pagination", None) or {}).get("nextToken")
        if not token or len(plans) >= max_plans:
            break

    agg, by_status, nship, awd = {}, {}, 0, 0
    for p in plans[:max_plans]:
        pid = p.get("inboundPlanId")
        if not pid:
            continue
        try:
            det = (_with_retry(lambda: g.get_inbound_plan(inboundPlanId=pid)).payload) or {}
        except SellingApiException as e:
            # AWD inbound plans appear in the list but can't be fetched by this API.
            if "Amazon Warehousing and Distribution" not in str(e):
                raise
            awd += 1
            continue
        for sh in (det.get("shipments") or []):
            st = (sh.get("status") or "").upper()
            by_status[st] = by_status.get(st, 0)
            if st not in want:
                continue
            sid = sh.get("shipmentId")
            if not sid:
                continue
            full = (_with_retry(lambda: g.get_shipment(inboundPlanId=pid, shipmentId=sid)).payload) or {}
            fc = ((full.get("destination") or {}).get("warehouseId")) or "UNKNOWN"
            nship += 1
            itok, ip = None, 0
            while ip < 20:
                ip += 1
                ikw = {"inboundPlanId": pid, "shipmentId": sid}
                if itok:
                    ikw["paginationToken"] = itok
                resp = _with_retry(lambda: g.list_shipment_items(**ikw))
                res = resp.payload or {}
                for it in (res.get("items") or []):
                    sku = it.get("msku")
                    if not sku:
                        continue
                    q = int(it.get("quantity") or 0)
                    agg[(fc, sku)] = agg.get((fc, sku), 0) + q
                    by_status[st] = by_status.get(st, 0) + q
                itok = (getattr(resp, "pagination", None) or {}).get("nextToken")
                if not itok:
                    break

    rows = [{"fc": fc, "sku": sku, "qty": q} for (fc, sku), q in sorted(agg.items())]
    return {"rows": rows, "plans": len(plans), "shipments": nship, "skipped_awd_plans": awd,
            "by_status": by_status, "total": sum(r["qty"] for r in rows)}


@mcp.tool()
@tool_safe
def get_confirmed_inbound_pickups(fcs: str = "", statuses: str = "READY_TO_SHIP",
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
    from sp_api.api import FulfillmentInboundV20240320 as G
    g = G(credentials=creds(), marketplace=MARKETPLACE_ENUM)
    want_st = {s.strip().upper() for s in statuses.split(",") if s.strip()}
    want_fc = {s.strip().upper() for s in fcs.split(",") if s.strip()}
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    def _ts(v):
        try:
            d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except Exception:  # noqa: BLE001
            return None

    out, scanned, skipped, pages, token, done = [], 0, 0, 0, None, False
    while pages < 20 and not done:
        pages += 1
        kw = {"pageSize": 30}
        if token:
            kw["paginationToken"] = token
        else:
            kw.update(status="ACTIVE", sortBy="CREATION_TIME", sortOrder="DESC")
        resp = _with_retry(lambda: g.list_inbound_plans(**kw))
        for p in ((resp.payload or {}).get("inboundPlans") or []):
            created = _ts(p.get("createdAt"))
            if created and created < cutoff:
                done = True
                break
            pid = p.get("inboundPlanId")
            if not pid:
                continue
            try:
                det = (_with_retry(lambda: g.get_inbound_plan(inboundPlanId=pid)).payload) or {}
            except SellingApiException as e:
                if getattr(e, "code", None) == 400:
                    skipped += 1
                    continue
                raise
            scanned += 1
            for sh in (det.get("shipments") or []):
                sid = sh.get("shipmentId")
                if not sid:
                    continue
                st = (sh.get("status") or "").upper()
                if not st or st not in want_st:
                    if st:
                        continue
                full = (_with_retry(lambda: g.get_shipment(inboundPlanId=pid, shipmentId=sid)).payload) or {}
                if not st:
                    st = (full.get("status") or "").upper()
                    if st not in want_st:
                        continue
                fc = ((full.get("destination") or {}).get("warehouseId")) or ""
                if want_fc and fc.upper() not in want_fc:
                    continue
                sel = full.get("selectedTransportationOptionId")
                opt, otok, op = {}, None, 0
                while op < 10 and not opt:
                    op += 1
                    okw = {"inboundPlanId": pid, "shipmentId": sid}
                    if otok:
                        okw["paginationToken"] = otok
                    r = _with_retry(lambda: g.list_transportation_options(**okw))
                    for o in ((r.payload or {}).get("transportationOptions") or []):
                        if o.get("transportationOptionId") == sel:
                            opt = o
                            break
                    otok = r.next_token
                    if not otok:
                        break
                ap = opt.get("carrierAppointment") or {}
                qt = opt.get("quote") or {}
                out.append({
                    "plan_id": pid, "shipment_id": sid,
                    "confirmation_id": full.get("shipmentConfirmationId"),
                    "status": st, "fc": fc, "transportation_option_id": sel,
                    "carrier": (opt.get("carrier") or {}).get("alphaCode"),
                    "shipping_solution": opt.get("shippingSolution"),
                    "appt_start": ap.get("startTime"), "appt_end": ap.get("endTime"),
                    "voidable_until": qt.get("voidableUntil"),
                    "cost_inr": (qt.get("cost") or {}).get("amount"),
                })
        token = resp.next_token
        if not token:
            break
    return {"shipments": out, "plans_scanned": scanned, "pages": pages, "skipped": skipped}


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


# --- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
    )
