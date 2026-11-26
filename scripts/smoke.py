"""Smoke-test a running amazon-sp-mcp container: python scripts/smoke.py http://localhost:3000"""
import asyncio
import sys
import urllib.request

from fastmcp import Client

EXPECTED = {
    "get_orders", "get_order", "get_order_items", "get_inventory_summary",
    "get_sales_metrics", "get_fba_reimbursements", "get_settlement_report",
    "get_fba_storage_fees", "get_fba_long_term_storage_fees", "get_inventory_ledger",
    "get_fulfilled_shipments", "get_report", "get_inbound_by_fc",
    "get_sales_traffic_report", "get_financial_events", "get_fee_estimate",
    "get_catalog_item", "search_catalog", "get_inbound_plans", "get_inbound_shipment_items",
    "get_product_pricing_for_skus", "get_listings_item",
}


async def main(base):
    base = base.rstrip("/")
    async with Client(f"{base}/mcp") as c:  # connect = MCP initialize
        names = {t.name for t in await c.list_tools()}
    missing = EXPECTED - names
    assert not missing, f"missing tools: {sorted(missing)}"
    with urllib.request.urlopen(f"{base}/health", timeout=10) as r:
        assert r.status == 200, f"/health returned {r.status}"
    print(f"OK: {len(names)} tools, /health 200")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: smoke.py <base_url>")
    try:
        asyncio.run(main(sys.argv[1]))
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
