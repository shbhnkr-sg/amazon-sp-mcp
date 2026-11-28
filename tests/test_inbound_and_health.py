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
        ok({"inboundPlans": [{"inboundPlanId": "P2"}]}),
        ok({"inboundPlans": []})])  # the SHIPPED listing
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


async def test_inbound_by_fc_skips_awd_plans(api):
    mock_one_plan(api)
    api.get(f"{IB}/inboundPlans").mock(return_value=ok({
        "inboundPlans": [{"inboundPlanId": "AWD1"}, {"inboundPlanId": "P1"}]}))
    api.get(f"{IB}/inboundPlans/AWD1").mock(return_value=httpx.Response(400, json={
        "errors": [{"code": "BadRequest", "message": "ERROR: Operation GetInboundPlan is not "
                    "supported for Amazon Warehousing and Distribution inbound plans."}]}))
    res = await call("get_inbound_by_fc")
    assert res["skipped_awd_plans"] == 1
    assert res["total"] == 12


# --- get_inbound_plans / get_inbound_shipment_items -------------------------------

def plan(pid, created="2099-01-01T00:00:00Z"):
    return {"inboundPlanId": pid, "createdAt": created}


def mock_plan_detail(api, pid, shipments, status="ACTIVE"):
    api.get(f"{IB}/inboundPlans/{pid}").mock(return_value=ok({
        "inboundPlanId": pid, "status": status, "createdAt": "2099-01-01T00:00:00Z",
        "shipments": [{"shipmentId": sid, "status": st} for sid, st, _ in shipments]}))
    for sid, _, full in shipments:
        api.get(f"{IB}/inboundPlans/{pid}/shipments/{sid}").mock(return_value=ok(full))


async def test_inbound_plans_pages_by_next_token_per_status_and_dedupes(api):
    route = api.get(f"{IB}/inboundPlans").mock(side_effect=[
        ok({"inboundPlans": [plan("P1")], "pagination": {"nextToken": "N"}}),
        ok({"inboundPlans": [plan("P2")]}),
        ok({"inboundPlans": [plan("P2"), plan("P3")]})])
    full = {"shipmentId": "S", "status": "IN_TRANSIT", "shipmentConfirmationId": "FBA1",
            "destination": {"warehouseId": "BOM5"}}
    for pid in ("P1", "P2", "P3"):
        mock_plan_detail(api, pid, [("S", "SHIPPED", full)])
    res = await call("get_inbound_plans", statuses="ACTIVE,SHIPPED")
    assert [p["plan_id"] for p in res["plans"]] == ["P1", "P2", "P3"]
    assert res["pages"] == 3 and res["skipped"] == 0
    assert [params(route, i)["status"] for i in range(3)] == [["ACTIVE"], ["ACTIVE"], ["SHIPPED"]]
    assert params(route, 1)["paginationToken"] == ["N"]
    assert res["plans"][0] == {
        "plan_id": "P1", "status": "ACTIVE", "created_at": "2099-01-01T00:00:00Z",
        "shipments": [{"shipment_id": "S", "status": "IN_TRANSIT", "confirmation_id": "FBA1",
                       "destination": "BOM5"}]}


async def test_inbound_plans_skip_old_and_awd_and_fall_back_to_listed_status(api):
    api.get(f"{IB}/inboundPlans").mock(return_value=ok({"inboundPlans": [
        plan("OLD", "2001-01-01T00:00:00Z"), plan("AWD1"), plan("P1")]}))
    api.get(f"{IB}/inboundPlans/OLD").mock(side_effect=AssertionError("old plan fetched"))
    api.get(f"{IB}/inboundPlans/AWD1").mock(return_value=httpx.Response(400, json={
        "errors": [{"code": "BadRequest", "message": "not supported for AWD plans"}]}))
    mock_plan_detail(api, "P1", [("S1", "SHIPPED", {"shipmentId": "S1"})])
    res = await call("get_inbound_plans", statuses="ACTIVE", days=30)
    assert [p["plan_id"] for p in res["plans"]] == ["P1"]
    assert res["skipped"] == 1
    assert res["plans"][0]["shipments"] == [
        {"shipment_id": "S1", "status": "SHIPPED", "confirmation_id": None, "destination": None}]


async def test_inbound_plans_repeated_token_is_an_error(api):
    api.get(f"{IB}/inboundPlans").mock(return_value=ok(
        {"inboundPlans": [], "pagination": {"nextToken": "SAME"}}))
    res = await call("get_inbound_plans", statuses="ACTIVE")
    assert "repeated" in res["error"]["message"]


async def test_inbound_plans_by_id_skips_listing(api):
    api.get(f"{IB}/inboundPlans").mock(side_effect=AssertionError("listed"))
    mock_plan_detail(api, "P9", [("S9", "READY_TO_SHIP", {"shipmentId": "S9", "status": "SHIPPED"})],
                     status="SHIPPED")
    res = await call("get_inbound_plans", plan_ids="P9")
    assert res["plans"][0]["status"] == "SHIPPED"
    assert res["plans"][0]["shipments"][0]["status"] == "SHIPPED"


V0 = f"{EU}/fba/inbound/v0/shipments"


async def test_shipment_items_v0_has_received(api):
    route = api.get(f"{V0}/FBA1/items").mock(return_value=ok({"payload": {"ItemData": [
        {"SellerSKU": "SKU1", "QuantityShipped": 5, "QuantityReceived": 2}]}}))
    res = await call("get_inbound_shipment_items", plan_id="P1", shipment_id="S1",
                     confirmation_id="FBA1")
    assert res == {"items": [{"msku": "SKU1", "shipped": 5, "received": 2}], "source": "v0"}
    assert params(route)["MarketplaceId"] == ["A21TJRUUN4KGV"]


async def test_shipment_items_fall_back_to_v2024_when_v0_fails(api):
    api.get(f"{V0}/FBA1/items").mock(return_value=httpx.Response(400, json={
        "errors": [{"code": "InvalidInput", "message": "unknown shipment"}]}))
    route = api.get(f"{IB}/inboundPlans/P1/shipments/S1/items").mock(side_effect=[
        ok({"items": [{"msku": "SKU1", "quantity": 5}], "pagination": {"nextToken": "T"}}),
        ok({"items": [{"msku": "SKU2", "quantity": 4}]})])
    res = await call("get_inbound_shipment_items", plan_id="P1", shipment_id="S1",
                     confirmation_id="FBA1")
    assert res["items"] == [{"msku": "SKU1", "shipped": 5, "received": 0},
                            {"msku": "SKU2", "shipped": 4, "received": 0}]
    assert res["source"] == "v2024" and "unknown shipment" in res["v0_error"]
    assert params(route, 1)["paginationToken"] == ["T"]


async def test_shipment_items_without_confirmation_id_skip_v0(api):
    api.get(f"{IB}/inboundPlans/P1/shipments/S1/items").mock(return_value=ok(
        {"items": [{"msku": "SKU1", "quantity": 3}]}))
    res = await call("get_inbound_shipment_items", plan_id="P1", shipment_id="S1")
    assert res["source"] == "v2024" and res["items"][0]["shipped"] == 3


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
    "get_catalog_item", "search_catalog", "get_confirmed_inbound_pickups",
    "get_inbound_plans", "get_inbound_shipment_items", "get_product_pricing_for_skus",
    "get_listings_item",
}
