import os
import time
from datetime import datetime, timedelta, timezone

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
from sp_api.base import Marketplaces, Granularity
from sp_api.base.exceptions import SellingApiException

load_dotenv()

mcp = FastMCP("amazon-sp")


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


# --- Health check (so the Docker HEALTHCHECK on /health passes) ----------------

@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


# --- Orders -------------------------------------------------------------------

@mcp.tool()
def get_orders(days_ago: int = 7, status: str = "Unshipped,PartiallyShipped,Shipped") -> dict:
    """Get orders from the last N days. Status can be comma-separated list."""
    created_after = _iso(datetime.now(timezone.utc) - timedelta(days=days_ago))
    try:
        res = Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_orders(
            CreatedAfter=created_after,
            OrderStatuses=status.split(","),
        )
        return res.payload
    except SellingApiException as e:
        return {"error": str(e)}


@mcp.tool()
def get_order(order_id: str) -> dict:
    """Get full details for a specific order ID."""
    try:
        return Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order(order_id).payload
    except SellingApiException as e:
        return {"error": str(e)}


@mcp.tool()
def get_order_items(order_id: str) -> dict:
    """Get line items for a specific order."""
    try:
        return Orders(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order_items(order_id).payload
    except SellingApiException as e:
        return {"error": str(e)}


# --- Inventory (FBA) ----------------------------------------------------------

@mcp.tool()
def get_inventory_summary(seller_sku: str = None) -> dict:
    """
    Get FBA inventory summary. Optionally filter by seller SKU.
    Returns quantities: fulfillable, inbound, reserved, unfulfillable.
    """
    try:
        kwargs = dict(details=True, granularityType="Marketplace", granularityId=MARKETPLACE)
        if seller_sku:
            kwargs["sellerSkus"] = [seller_sku]
        return Inventories(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_inventory_summary_marketplace(**kwargs).payload
    except SellingApiException as e:
        return {"error": str(e)}


# --- Sales metrics ------------------------------------------------------------

@mcp.tool()
def get_sales_metrics(days_ago: int = 30, granularity: str = "Day") -> dict:
    """
    Get sales aggregates. granularity = Day | Week | Month.
    Returns ordered units, order count, average selling price, average units per order.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_ago)
    try:
        gran = Granularity[granularity.upper()]
    except KeyError:
        valid = ", ".join(g.name.title() for g in Granularity)
        return {"error": f"Invalid granularity {granularity!r}; use one of: {valid}"}
    try:
        res = Sales(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_order_metrics(
            interval=(_iso(start), _iso(end)),
            granularity=gran,
            granularityTimeZone="Asia/Kolkata",
            marketplaceIds=[MARKETPLACE],
        )
        # get_order_metrics returns a JSON array; MCP structured output needs a
        # top-level object, so wrap the list under a key.
        return {"metrics": res.payload}
    except SellingApiException as e:
        return {"error": str(e)}


# --- Reports (async — request → poll → download) ------------------------------

def _request_and_fetch_report(report_type: str, days_ago: int = 30, extra_options: dict = None) -> dict:
    """Internal helper: request a report, poll until done, return parsed content."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_ago)
    opts = {
        "reportType": report_type,
        "marketplaceIds": [MARKETPLACE],
        "dataStartTime": _iso(start),
        "dataEndTime": _iso(end),
    }
    if extra_options:
        opts.update(extra_options)
    try:
        r = Reports(credentials=creds(), marketplace=MARKETPLACE_ENUM)
        report = r.create_report(**opts)
        report_id = report.payload["reportId"]

        # Poll until status is DONE (max 120s)
        for _ in range(24):
            time.sleep(5)
            status = r.get_report(report_id).payload
            if status["processingStatus"] == "DONE":
                doc_id = status["reportDocumentId"]
                doc = r.get_report_document(doc_id, decrypt=True)
                return {"reportId": report_id, "content": doc.payload}
            if status["processingStatus"] in ("CANCELLED", "FATAL"):
                return {"error": f"Report {status['processingStatus']}", "reportId": report_id}

        return {"error": "Timed out waiting for report", "reportId": report_id}
    except SellingApiException as e:
        return {"error": str(e)}


@mcp.tool()
def get_fba_reimbursements(days_ago: int = 60) -> dict:
    """Get FBA reimbursements for lost or damaged inventory in the last N days."""
    return _request_and_fetch_report("GET_FBA_REIMBURSEMENTS_DATA", days_ago)


@mcp.tool()
def get_settlement_report(days_ago: int = 30) -> dict:
    """Get settlement/disbursement report showing payment breakdown."""
    return _request_and_fetch_report("GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE", days_ago)


@mcp.tool()
def get_fba_storage_fees(days_ago: int = 30) -> dict:
    """Get FBA monthly storage fee charges by SKU/ASIN."""
    return _request_and_fetch_report("GET_FBA_STORAGE_FEE_CHARGES_DATA", days_ago)


@mcp.tool()
def get_fba_long_term_storage_fees() -> dict:
    """Get FBA long-term storage fees (inventory aged 180+ and 365+ days)."""
    return _request_and_fetch_report("GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA", days_ago=180)


@mcp.tool()
def get_inventory_ledger(days_ago: int = 30) -> dict:
    """Get inventory ledger report — tracks all inventory movements (receipts, shipments, removals, adjustments)."""
    return _request_and_fetch_report("GET_LEDGER_SUMMARY_VIEW_DATA", days_ago)


@mcp.tool()
def get_sales_traffic_report(days_ago: int = 30) -> dict:
    """Get sales & traffic report — sessions, page views, buy box %, conversion rate by ASIN."""
    return _request_and_fetch_report("GET_SALES_AND_TRAFFIC_REPORT", days_ago)


# --- Fee estimates ------------------------------------------------------------

@mcp.tool()
def get_fee_estimate(asin: str, price: float) -> dict:
    """
    Get FBA fee estimate for an ASIN at a given selling price (INR).
    Returns referral fee, FBA fulfillment fee, total estimated fees.
    """
    try:
        return ProductFees(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_product_fees_estimate_for_asin(
            asin, price, currency="INR", is_fba=True
        ).payload
    except SellingApiException as e:
        return {"error": str(e)}


# --- Catalog / listings -------------------------------------------------------

@mcp.tool()
def get_catalog_item(asin: str) -> dict:
    """Get product details for an ASIN — title, brand, dimensions, images."""
    try:
        # The library's CatalogItems uses API version 2020-12-01, where valid
        # includedData excludes "dimensions" (a 2022-04-01 field). Dimension
        # data is returned within "attributes" instead.
        return CatalogItems(credentials=creds(), marketplace=MARKETPLACE_ENUM).get_catalog_item(
            asin=asin,
            marketplaceIds=[MARKETPLACE],
            includedData=["attributes", "summaries", "images"],
        ).payload
    except SellingApiException as e:
        return {"error": str(e)}


@mcp.tool()
def search_catalog(keywords: str, page_size: int = 10) -> dict:
    """Search catalog by keywords. Returns ASINs, titles, and basic product info."""
    try:
        return CatalogItems(credentials=creds(), marketplace=MARKETPLACE_ENUM).search_catalog_items(
            keywords=keywords,
            marketplaceIds=[MARKETPLACE],
            includedData=["summaries"],
            pageSize=page_size,
        ).payload
    except SellingApiException as e:
        return {"error": str(e)}


# --- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
    )
