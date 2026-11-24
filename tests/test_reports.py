import json

import httpx
import pytest

import server
from conftest import EU, call, params

pytestmark = pytest.mark.anyio

MP = "A21TJRUUN4KGV"
R = f"{EU}/reports/2021-06-30"
TSV = "sku\tqty\tcity\nA\t1\tDelhi\nB\t2\tMumbai\nC\t3\tPune\n"


def ok(js):
    return httpx.Response(200, json=js)


def mock_report(api, doc_url, *, compression=None, report_id="R1", doc_id="D1"):
    """Mock create -> status DONE -> document descriptor pointing at doc_url."""
    create = api.post(f"{R}/reports").mock(return_value=ok({"reportId": report_id}))
    status = api.get(f"{R}/reports/{report_id}").mock(return_value=ok(
        {"reportId": report_id, "processingStatus": "DONE", "reportDocumentId": doc_id}))
    desc = {"reportDocumentId": doc_id, "url": doc_url}
    if compression:
        desc["compressionAlgorithm"] = compression
    doc = api.get(f"{R}/documents/{doc_id}").mock(return_value=ok(desc))
    return create, status, doc


def create_body(route):
    return json.loads(route.calls.last.request.content)


# Every report tool: (tool, args, expected reportType)
REPORT_TOOLS = [
    ("get_fba_reimbursements", {}, "GET_FBA_REIMBURSEMENTS_DATA"),
    ("get_fba_storage_fees", {}, "GET_FBA_STORAGE_FEE_CHARGES_DATA"),
    ("get_fba_long_term_storage_fees", {}, "GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA"),
    ("get_inventory_ledger", {}, "GET_LEDGER_SUMMARY_VIEW_DATA"),
    ("get_fulfilled_shipments", {}, "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL"),
    ("get_report", {"report_type": "GET_MERCHANT_LISTINGS_ALL_DATA"}, "GET_MERCHANT_LISTINGS_ALL_DATA"),
]


@pytest.mark.parametrize("tool,args,rtype", REPORT_TOOLS)
async def test_report_tool_request_and_plain_download(api, doc_host, tool, args, rtype):
    url = doc_host("/doc", TSV.encode())
    create, status, doc = mock_report(api, url)
    res = await call(tool, **args)
    body = create_body(create)
    assert body["reportType"] == rtype
    assert body["marketplaceIds"] == [MP]
    assert body["dataStartTime"].endswith("Z") and body["dataEndTime"].endswith("Z")
    assert status.called and doc.called
    assert res["format"] == "tsv" and res["rows"] == 3
    assert res["content"] == TSV.rstrip("\n")
    assert res["reportId"] == "R1" and res["truncated"] is False


async def test_report_gzip_download(api, doc_host):
    url = doc_host("/doc.gz", TSV.encode(), gz=True)
    mock_report(api, url, compression="GZIP")
    res = await call("get_fba_reimbursements")
    assert res["content"] == TSV.rstrip("\n")


async def test_report_cp1252_document_decodes(api, doc_host):
    url = doc_host("/doc", "sku\tname\nA\tcaf\xe9\n".encode("cp1252"))
    mock_report(api, url)
    res = await call("get_report", report_type="X")
    assert "caf\xe9" in res["content"]


async def test_report_max_rows_and_field_projection(api, doc_host):
    url = doc_host("/doc", TSV.encode())
    mock_report(api, url)
    res = await call("get_fba_reimbursements", max_rows=2, fields="sku,city")
    assert res["rows"] == 2 and res["truncated"] is True
    assert res["content"] == "sku\tcity\nA\tDelhi\nB\tMumbai"


async def test_inventory_ledger_report_options_and_ist_window(api, doc_host):
    mock_report_ = mock_report(api, doc_host("/doc", TSV.encode()))
    await call("get_inventory_ledger", after="2026-01-01", before="2026-01-05",
               aggregated_by="MONTHLY", aggregate_by_location="COUNTRY")
    body = create_body(mock_report_[0])
    # quirk: reportOptions forwarded verbatim in the create_report body
    assert body["reportOptions"] == {"aggregateByLocation": "COUNTRY",
                                     "aggregatedByTimePeriod": "MONTHLY"}
    assert body["dataStartTime"] == "2025-12-31T18:30:00Z"
    assert body["dataEndTime"] == "2026-01-04T18:30:00Z"


async def test_inventory_ledger_default_is_daily_by_fc(api, doc_host):
    create, *_ = mock_report(api, doc_host("/doc", TSV.encode()))
    await call("get_inventory_ledger")
    assert create_body(create)["reportOptions"] == {"aggregateByLocation": "FC",
                                                    "aggregatedByTimePeriod": "DAILY"}


async def test_get_report_passes_report_options(api, doc_host):
    create, *_ = mock_report(api, doc_host("/doc", TSV.encode()))
    await call("get_report", report_type="GET_X", report_options={"k": "v"})
    assert create_body(create)["reportOptions"] == {"k": "v"}


SAT = {"reportSpecification": {"reportType": "GET_SALES_AND_TRAFFIC_REPORT"},
       "salesAndTrafficByDate": [{"date": "2026-01-01", "salesByDate": {}}],
       "salesAndTrafficByAsin": [{"parentAsin": f"B{i}"} for i in range(3)]}


@pytest.mark.parametrize("by,has_date,has_asin", [
    ("date", True, False), ("asin", False, True), ("both", True, True)])
async def test_sales_traffic_report_by(api, doc_host, by, has_date, has_asin):
    mock_report(api, doc_host("/doc", json.dumps(SAT).encode()))
    res = await call("get_sales_traffic_report", by=by, max_rows=2)
    assert res["format"] == "json"
    assert ("salesAndTrafficByDate" in res) is has_date
    assert ("salesAndTrafficByAsin" in res) is has_asin
    if has_asin:
        assert len(res["salesAndTrafficByAsin"]) == 2
        assert res["asinCount"] == 3 and res["asinTruncated"] is True


async def test_sales_traffic_gzip_json(api, doc_host):
    mock_report(api, doc_host("/doc", json.dumps(SAT).encode(), gz=True), compression="GZIP")
    res = await call("get_sales_traffic_report")
    assert res["salesAndTrafficByDate"][0]["date"] == "2026-01-01"


async def test_resume_with_report_id_skips_create(api, doc_host):
    create, status, doc = mock_report(api, doc_host("/doc", TSV.encode()), report_id="R9")
    res = await call("get_fba_reimbursements", report_id="R9")
    assert not create.called
    assert status.called and res["reportId"] == "R9"


async def test_report_in_progress_envelope(api):
    api.post(f"{R}/reports").mock(return_value=ok({"reportId": "R2"}))
    api.get(f"{R}/reports/R2").mock(return_value=ok(
        {"reportId": "R2", "processingStatus": "IN_PROGRESS"}))
    res = await call("get_fba_reimbursements")
    assert res["status"] == "IN_PROGRESS" and res["reportId"] == "R2"


async def test_report_cancelled_is_no_data_not_error(api):
    api.post(f"{R}/reports").mock(return_value=ok({"reportId": "R3"}))
    api.get(f"{R}/reports/R3").mock(return_value=ok(
        {"reportId": "R3", "processingStatus": "CANCELLED"}))
    res = await call("get_fba_reimbursements")
    assert res["status"] == "NO_DATA" and "error" not in res


async def test_report_fatal_is_error(api):
    api.post(f"{R}/reports").mock(return_value=ok({"reportId": "R4"}))
    api.get(f"{R}/reports/R4").mock(return_value=ok(
        {"reportId": "R4", "processingStatus": "FATAL"}))
    res = await call("get_fba_reimbursements")
    assert res["error"]["type"] == "ReportError"


async def test_settlement_report_picks_latest_done(api, doc_host):
    listing = api.get(f"{R}/reports").mock(return_value=ok({"reports": [
        {"reportId": "old", "processingStatus": "DONE", "reportDocumentId": "DOLD",
         "dataEndTime": "2026-01-01T00:00:00Z"},
        {"reportId": "new", "processingStatus": "DONE", "reportDocumentId": "DNEW",
         "dataEndTime": "2026-02-01T00:00:00Z"},
        {"reportId": "pending", "processingStatus": "IN_PROGRESS",
         "dataEndTime": "2026-03-01T00:00:00Z"}]}))
    doc = api.get(f"{R}/documents/DNEW").mock(
        return_value=ok({"reportDocumentId": "DNEW", "url": doc_host("/s", TSV.encode())}))
    res = await call("get_settlement_report")
    assert "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE" in str(
        params(listing)["reportTypes"])
    assert doc.called
    assert res["reportId"] == "new" and res["dataEndTime"] == "2026-02-01T00:00:00Z"
    assert res["rows"] == 3


async def test_settlement_report_none_available(api):
    api.get(f"{R}/reports").mock(return_value=ok({"reports": []}))
    res = await call("get_settlement_report")
    assert res["error"]["type"] == "ReportError"


def test_extract_report_text_prefers_inline_document():
    assert server._extract_report_text({"document": "inline"}) == "inline"
