import httpx
import pytest
from starlette.testclient import TestClient

import server
from conftest import EU, call, params

pytestmark = pytest.mark.anyio

IB = f"{EU}/inbound/fba/2024-03-20"


def ok(js):
    return httpx.Response(200, json=js)


def mock_one_plan(api, *, status="SHIPPED"):
    api.get(f"{IB}/inboundPlans").mock(return_value=ok({"inboundPlans": [{"inboundPlanId": "P1"}]}))
    api.get(f"{IB}/inboundPlans/P1").mock(return_value=ok({
        "inboundPlanId": "P1", "shipments": [{"shipmentId": "S1", "status": status}]}))
    api.get(f"{IB}/inboundPlans/P1/shipments/S1").mock(return_value=ok({
        "shipmentId": "S1", "destination": {"warehouseId": "BOM5"}}))
    return api.get(f"{IB}/inboundPlans/P1/shipments/S1/items").mock(return_value=ok({
        "items": [{"msku": "SKU1", "quantity": 5}, {"msku": "SKU2", "quantity": 7},
                  {"quantity": 9}]}))


async def test_inbound_by_fc_aggregates_per_fc_and_sku(api):
    items = mock_one_plan(api)
    res = await call("get_inbound_by_fc")
    assert res["rows"] == [{"fc": "BOM5", "sku": "SKU1", "qty": 5},
                           {"fc": "BOM5", "sku": "SKU2", "qty": 7}]
    assert res["plans"] == 1 and res["shipments"] == 1 and res["total"] == 12
    assert res["by_status"] == {"SHIPPED": 12}
    assert items.called


async def test_inbound_plans_request_params(api):
    mock_one_plan(api)
    await call("get_inbound_by_fc")
    req = [c.request for r in api.routes for c in r.calls
           if c.request.url.path == "/inbound/fba/2024-03-20/inboundPlans"][0]
    assert req.method == "GET"
    assert req.url.params["pageSize"] == "30"


async def test_inbound_by_fc_status_filter_skips_shipment_fetch(api):
    mock_one_plan(api, status="CLOSED")
    api.get(f"{IB}/inboundPlans/P1/shipments/S1").mock(side_effect=AssertionError("fetched"))
    res = await call("get_inbound_by_fc")
    assert res["rows"] == [] and res["shipments"] == 0


# The library hoists a top-level `pagination` key off .payload onto resp.pagination
# (payload-or-kwargs quirk), so the token must be read from the response object.
async def test_inbound_plans_follow_pagination(api):
    route = api.get(f"{IB}/inboundPlans").mock(side_effect=[
        ok({"inboundPlans": [{"inboundPlanId": "P1"}], "pagination": {"nextToken": "N"}}),
        ok({"inboundPlans": [{"inboundPlanId": "P2"}]})])
    api.get(f"{IB}/inboundPlans/P1").mock(return_value=ok({"shipments": []}))
    api.get(f"{IB}/inboundPlans/P2").mock(return_value=ok({"shipments": []}))
    res = await call("get_inbound_by_fc")
    assert res["plans"] == 2
    assert params(route, 1)["paginationToken"] == ["N"]


async def test_shipment_items_follow_pagination(api):
    mock_one_plan(api)
    route = api.get(f"{IB}/inboundPlans/P1/shipments/S1/items").mock(side_effect=[
        ok({"items": [{"msku": "SKU1", "quantity": 5}], "pagination": {"nextToken": "T"}}),
        ok({"items": [{"msku": "SKU1", "quantity": 4}]})])
    res = await call("get_inbound_by_fc")
    assert res["rows"] == [{"fc": "BOM5", "sku": "SKU1", "qty": 9}]
    assert params(route, 1)["paginationToken"] == ["T"]


def test_health_endpoint_over_http():
    app = server.mcp.http_app(transport="streamable-http", path="/mcp")
    with TestClient(app) as c:
        r = c.get("/health")
    assert r.status_code == 200 and r.text == "OK"


async def test_all_tools_registered():
    from fastmcp import Client
    async with Client(server.mcp) as c:
        names = {t.name for t in await c.list_tools()}
    assert names == EXPECTED_TOOLS


EXPECTED_TOOLS = {
    "get_orders", "get_order", "get_order_items", "get_inventory_summary",
    "get_sales_metrics", "get_fba_reimbursements", "get_settlement_report",
    "get_fba_storage_fees", "get_fba_long_term_storage_fees", "get_inventory_ledger",
    "get_fulfilled_shipments", "get_report", "get_inbound_by_fc",
    "get_sales_traffic_report", "get_financial_events", "get_fee_estimate",
    "get_catalog_item", "search_catalog",
}
