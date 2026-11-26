import re

import httpx
import pytest

import server
from conftest import EU, LWA, call, params

pytestmark = pytest.mark.anyio

ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
MP = "A21TJRUUN4KGV"


def ok(json):
    return httpx.Response(200, json=json)


# --- auth / endpoint / marketplace quirks ---------------------------------------

def test_marketplace_resolves_to_enum_for_india():
    assert server.MARKETPLACE_ENUM.name == "IN"
    assert server.MARKETPLACE == MP
    assert server.MARKETPLACE_ENUM.endpoint == EU


async def test_lwa_token_exchange_and_eu_endpoint(api):
    route = api.get(f"{EU}/orders/v0/orders/123-1").mock(
        return_value=ok({"payload": {"AmazonOrderId": "123-1"}}))
    await call("get_order", order_id="123-1")
    lwa = api.lwa.calls.last.request
    assert lwa.url == LWA
    body = lwa.content.decode()
    assert "grant_type=refresh_token" in body
    assert "refresh_token=test-refresh-token" in body
    assert "client_id=test-client-id" in body
    sp = route.calls.last.request
    assert sp.headers["x-amz-access-token"] == "tok-123"
    assert sp.url.host == "sellingpartnerapi-eu.amazon.com"


async def test_unknown_marketplace_rejected():
    with pytest.raises(ValueError):
        server._resolve_marketplace("NOPE")


# --- Orders ---------------------------------------------------------------------

async def test_get_order(api):
    api.get(f"{EU}/orders/v0/orders/123-1").mock(
        return_value=ok({"payload": {"AmazonOrderId": "123-1", "OrderStatus": "Shipped"}}))
    res = await call("get_order", order_id="123-1")
    assert res == {"AmazonOrderId": "123-1", "OrderStatus": "Shipped"}


async def test_get_orders_default_omits_created_before(api):
    route = api.get(f"{EU}/orders/v0/orders").mock(return_value=ok({"payload": {
        "Orders": [{"AmazonOrderId": "A", "OrderStatus": "Shipped", "Extra": 1}]}}))
    res = await call("get_orders", fields="AmazonOrderId,OrderStatus")
    p = params(route)
    assert ISO.match(p["CreatedAfter"][0])
    # quirk: CreatedBefore within ~2 min of now is rejected by Amazon -> never sent by default
    assert "CreatedBefore" not in p
    assert p["MarketplaceIds"] == [MP]
    assert ",".join(p["OrderStatuses"]) == "Unshipped,PartiallyShipped,Shipped"
    assert res == {"orders": [{"AmazonOrderId": "A", "OrderStatus": "Shipped"}],
                   "count": 1, "truncated": False, "next_token": None}


async def test_get_orders_explicit_range_filters_and_all_status(api):
    route = api.get(f"{EU}/orders/v0/orders").mock(
        return_value=ok({"payload": {"Orders": []}}))
    await call("get_orders", created_after="2026-01-01", created_before="2026-01-02",
               status="all", fulfillment_channels="AFN")
    p = params(route)
    assert p["CreatedAfter"] == ["2025-12-31T18:30:00Z"]  # IST midnight -> UTC
    assert p["CreatedBefore"] == ["2026-01-01T18:30:00Z"]
    assert "OrderStatuses" not in p
    assert ",".join(p["FulfillmentChannels"]) == "AFN"


async def test_get_orders_paginates_with_token_only(api):
    route = api.get(f"{EU}/orders/v0/orders").mock(side_effect=[
        ok({"payload": {"Orders": [{"AmazonOrderId": "1"}], "NextToken": "T1"}}),
        ok({"payload": {"Orders": [{"AmazonOrderId": "2"}]}}),
    ])
    res = await call("get_orders")
    assert [o["AmazonOrderId"] for o in res["orders"]] == ["1", "2"]
    assert route.call_count == 2
    p2 = params(route, 1)
    assert p2["NextToken"] == ["T1"]
    assert "CreatedAfter" not in p2  # Amazon rejects token + filters


async def test_get_orders_resumes_from_next_token_and_truncates(api):
    route = api.get(f"{EU}/orders/v0/orders").mock(return_value=ok({"payload": {
        "Orders": [{"AmazonOrderId": "1"}], "NextToken": "T2"}}))
    res = await call("get_orders", next_token="T1", max_orders=1)
    assert params(route, 0)["NextToken"] == ["T1"]
    assert route.call_count == 1
    assert res["truncated"] is True and res["next_token"] == "T2"


async def test_get_order_items(api):
    route = api.get(f"{EU}/orders/v0/orders/123-1/orderItems").mock(
        return_value=ok({"payload": {"AmazonOrderId": "123-1",
                                     "OrderItems": [{"SellerSKU": "S1", "QuantityOrdered": 2}]}}))
    res = await call("get_order_items", order_id="123-1")
    assert route.called
    assert res["items"] == [{"SellerSKU": "S1", "QuantityOrdered": 2}]
    assert res["order_id"] == "123-1" and res["count"] == 1


# --- Inventory ------------------------------------------------------------------

async def test_inventory_summary_request_and_pagination_from_resp_pagination(api):
    route = api.get(f"{EU}/fba/inventory/v1/summaries").mock(side_effect=[
        # quirk: pagination is a top-level key -> hoisted to resp.pagination, not in .payload
        ok({"payload": {"granularity": {}, "inventorySummaries": [{"sellerSku": "A"}]},
            "pagination": {"nextToken": "P2"}}),
        ok({"payload": {"granularity": {}, "inventorySummaries": [{"sellerSku": "B"}]}}),
    ])
    res = await call("get_inventory_summary", seller_skus="A, B")
    assert [i["sellerSku"] for i in res["inventory"]] == ["A", "B"]  # page 2 was fetched
    p1, p2 = params(route, 0), params(route, 1)
    assert p1["details"] == ["true"]
    assert p1["granularityType"] == ["Marketplace"]
    assert p1["granularityId"] == [MP]
    assert p1["marketplaceIds"] == [MP]
    assert ISO.match(p1["startDateTime"][0])
    assert ",".join(p1["sellerSkus"]) == "A,B"
    assert "nextToken" not in p1
    assert p2["nextToken"] == ["P2"]
    assert p2["startDateTime"] == p1["startDateTime"] or ISO.match(p2["startDateTime"][0])


async def test_inventory_start_date_within_18_months(api):
    from datetime import datetime, timezone
    route = api.get(f"{EU}/fba/inventory/v1/summaries").mock(
        return_value=ok({"payload": {"inventorySummaries": []}}))
    await call("get_inventory_summary")
    start = datetime.strptime(params(route)["startDateTime"][0], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - start).days
    assert 500 <= age_days <= 545


# --- Sales ----------------------------------------------------------------------

async def test_sales_metrics_enum_granularity_tuple_interval_and_list_wrap(api):
    # quirk: library returns a JSON list; payload-or-kwargs; server wraps under "metrics"
    route = api.get(f"{EU}/sales/v1/orderMetrics").mock(return_value=ok({"payload": [
        {"interval": "x", "unitCount": 3, "orderCount": 2}]}))
    res = await call("get_sales_metrics", days_ago=7, granularity="week")
    p = params(route)
    assert p["granularity"] == ["Week"]
    assert p["granularityTimeZone"] == ["Asia/Kolkata"]
    assert p["marketplaceIds"] == [MP]
    start, end = p["interval"][0].split("--")
    assert start.endswith("18:30:00Z") and end.endswith("18:30:00Z")  # IST midnight floor
    assert res == {"metrics": [{"interval": "x", "unitCount": 3, "orderCount": 2}]}


async def test_sales_metrics_empty_result_coerced_to_list(api):
    api.get(f"{EU}/sales/v1/orderMetrics").mock(return_value=ok({"payload": []}))
    assert await call("get_sales_metrics") == {"metrics": []}


async def test_sales_metrics_invalid_granularity(api):
    res = await call("get_sales_metrics", granularity="Fortnight")
    assert res["error"]["type"] == "ValueError"
    assert "Day" in res["error"]["message"]


# --- Fees -----------------------------------------------------------------------

async def test_fee_estimate(api):
    route = api.post(f"{EU}/products/fees/v0/items/B000TEST/feesEstimate").mock(
        return_value=ok({"payload": {"FeesEstimateResult": {"Status": "Success"}}}))
    res = await call("get_fee_estimate", asin="B000TEST", price=499.0, is_fba=False)
    import json
    body = json.loads(route.calls.last.request.content)
    est = body["FeesEstimateRequest"]
    assert est["MarketplaceId"] == MP
    assert est["IsAmazonFulfilled"] is False
    assert est["PriceToEstimateFees"]["ListingPrice"] == {"CurrencyCode": "INR", "Amount": 499.0}
    assert res["FeesEstimateResult"]["Status"] == "Success"


# --- Catalog --------------------------------------------------------------------

async def test_catalog_item_uses_2020_12_01_and_included_data(api):
    route = api.get(f"{EU}/catalog/2020-12-01/items/B000TEST").mock(
        return_value=ok({"asin": "B000TEST", "attributes": {"a": 1}, "summaries": [{"x": 1}]}))
    res = await call("get_catalog_item", asin="B000TEST", fields="asin,attributes")
    p = params(route)
    assert p["marketplaceIds"] == [MP]
    assert ",".join(p["includedData"]) == "attributes,summaries,images"
    assert res == {"asin": "B000TEST", "attributes": {"a": 1}}


async def test_search_catalog_keeps_keywords_and_reads_pagination_from_resp(api):
    route = api.get(f"{EU}/catalog/2020-12-01/items").mock(side_effect=[
        ok({"numberOfResults": 2, "items": [{"asin": "A"}], "pagination": {"nextToken": "N2"}}),
        ok({"numberOfResults": 2, "items": [{"asin": "B"}]}),
    ])
    r1 = await call("search_catalog", keywords="cotton shirt", page_size=1)
    p = params(route, 0)
    assert p["keywords"] == ["cotton shirt"]
    assert p["pageSize"] == ["1"]
    assert p["marketplaceIds"] == [MP]
    assert r1["items"] == [{"asin": "A"}]
    assert r1["next_token"] == "N2"  # quirk: lives on resp.pagination
    r2 = await call("search_catalog", keywords="cotton shirt", page_token="N2")
    p2 = params(route, 1)
    assert p2["pageToken"] == ["N2"]
    assert p2["keywords"] == ["cotton shirt"]  # keywords kept on token requests
    assert r2["next_token"] is None


# --- Finances -------------------------------------------------------------------

FIN = {"payload": {"FinancialEvents": {
    "ShipmentEventList": [{
        "PostedDate": "2026-01-02T00:00:00Z", "AmazonOrderId": "O1",
        "ShipmentItemList": [{
            "SellerSKU": "S1", "QuantityShipped": 1,
            "ItemChargeList": [
                {"ChargeType": "Principal", "ChargeAmount": {"CurrencyAmount": 100.0}},
                {"ChargeType": "Tax", "ChargeAmount": {"CurrencyAmount": 18.0}}],
            "ItemFeeList": [
                {"FeeType": "Commission", "FeeAmount": {"CurrencyAmount": -10.0}},
                {"FeeType": "FixedClosingFee", "FeeAmount": {"CurrencyAmount": -5.5}}]}]}],
    "RefundEventList": [{
        "PostedDate": "2026-01-03T00:00:00Z", "AmazonOrderId": "O2",
        "ShipmentItemAdjustmentList": [{
            "SellerSKU": "S2", "QuantityShipped": 1,
            "ItemChargeAdjustmentList": [
                {"ChargeType": "Principal", "ChargeAmount": {"CurrencyAmount": -50.0}}],
            "ItemFeeAdjustmentList": [
                {"FeeType": "Commission", "FeeAmount": {"CurrencyAmount": 5.0}}]}]}]},
    "NextToken": "F2"}}


async def test_financial_events_flattening_and_token_pagination(api):
    route = api.get(f"{EU}/finances/v0/financialEvents").mock(side_effect=[
        ok(FIN), ok({"payload": {"FinancialEvents": {}}})])
    res = await call("get_financial_events")
    p1, p2 = params(route, 0), params(route, 1)
    assert ISO.match(p1["PostedAfter"][0]) and ISO.match(p1["PostedBefore"][0])
    assert p1["MaxResultsPerPage"] == ["100"]
    assert p2["NextToken"] == ["F2"] and "PostedAfter" not in p2
    shp, ref = res["events"]
    assert shp == {"posted_date": "2026-01-02T00:00:00Z", "order_id": "O1", "sku": "S1",
                   "quantity": 1, "event_type": "shipment", "principal": 100.0, "tax": 18.0,
                   "fees": {"Commission": -10.0, "FixedClosingFee": -5.5}, "total_fees": -15.5}
    assert ref["event_type"] == "refund" and ref["principal"] == -50.0 and ref["total_fees"] == 5.0
    assert res["count"] == 2 and res["truncated"] is False


async def test_financial_events_event_type_filter(api):
    api.get(f"{EU}/finances/v0/financialEvents").mock(return_value=ok(
        {"payload": {**FIN["payload"], "NextToken": None}}))
    res = await call("get_financial_events", event_types="refund")
    assert [e["event_type"] for e in res["events"]] == ["refund"]


# --- Error envelope / retry -----------------------------------------------------

async def test_sp_api_error_becomes_envelope(api):
    api.get(f"{EU}/orders/v0/orders/bad").mock(return_value=httpx.Response(
        400, json={"errors": [{"code": "InvalidInput", "message": "bad id"}]}))
    res = await call("get_order", order_id="bad")
    assert res["error"]["code"] == 400
    assert "SellingApiBadRequestException" in res["error"]["type"]


async def test_throttle_429_is_retried_on_paginated_calls(api):
    route = api.get(f"{EU}/orders/v0/orders").mock(side_effect=[
        httpx.Response(429, json={"errors": [{"code": "QuotaExceeded", "message": "slow"}]}),
        ok({"payload": {"Orders": [{"AmazonOrderId": "1"}]}})])
    res = await call("get_orders")
    assert route.call_count == 2
    assert res["count"] == 1


# --- pricing / listings ----------------------------------------------------------

async def test_product_pricing_for_skus(api):
    route = api.get(f"{EU}/products/pricing/v0/price").mock(return_value=ok({"payload": [
        {"status": "Success", "SellerSKU": "SKU1", "Product": {"Offers": []}}]}))
    res = await call("get_product_pricing_for_skus", skus="SKU1, SKU2")
    assert res["pricing"][0]["SellerSKU"] == "SKU1"
    p = params(route)
    assert p["ItemType"] == ["Sku"] and p["MarketplaceId"] == [MP]
    assert p["Skus"] in (["SKU1,SKU2"], ["SKU1", "SKU2"])


async def test_product_pricing_rejects_more_than_20(api):
    res = await call("get_product_pricing_for_skus", skus=",".join(f"S{i}" for i in range(21)))
    assert "20" in res["error"]["message"]


async def test_listings_item_offers(api):
    route = api.get(f"{EU}/listings/2021-08-01/items/SELLER1/SKU1").mock(return_value=ok(
        {"sku": "SKU1", "offers": [{"price": {"currencyCode": "INR", "amount": "10.00"}}]}))
    res = await call("get_listings_item", seller_id="SELLER1", sku="SKU1", included_data="offers")
    assert res["offers"][0]["price"]["amount"] == "10.00"
    p = params(route)
    assert p["includedData"] == ["offers"] and p["marketplaceIds"] == [MP]
