"""Rate-safe, concurrent, incremental inbound walks: the token buckets and 429 handling,
order preservation under concurrency, the terminal-shipment cache, the sorted listing's
early stop, and get_inbound_by_fc's in-transit count. tests/fixtures/walk_old.json holds
the pre-change tools' outputs on tests/walk_fixture.py's account."""

import json
import threading
import time
from pathlib import Path

import httpx
import pytest

import server
import walk_fixture
from conftest import call

pytestmark = pytest.mark.anyio

REAL_SLEEP = time.sleep  # conftest patches time.sleep (it is server.time.sleep) per test
OLD = json.loads((Path(__file__).parent / "fixtures" / "walk_old.json").read_text())
THROTTLED = httpx.Response(429, json={"errors": [{"code": "QuotaExceeded", "message": "slow"}]})


def old(tool, args):
    return next(c["result"] for c in OLD if c["tool"] == tool and c["args"] == args)


def paths(calls, needle):
    return [p for p in calls if p.endswith(needle)]


# --- token bucket ---------------------------------------------------------------

class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_bucket_allows_the_burst_then_paces_at_the_rate():
    clk = Clock()
    b = server._Bucket(2, 6, clock=clk)
    assert [b.reserve() for _ in range(6)] == [0] * 6
    assert [b.reserve() for _ in range(3)] == [0.5, 1.0, 1.5]
    clk.t = 10.0  # refills to the burst, never past it
    assert [b.reserve() for _ in range(7)] == [0] * 6 + [0.5]


def test_bucket_drain_makes_every_caller_wait():
    clk = Clock()
    b = server._Bucket(0.5, 1, clock=clk)
    b.drain()
    assert b.reserve() == 2.0


def test_bucket_is_shared_safely_across_threads():
    clk = Clock()
    b = server._Bucket(2, 6, clock=clk)
    waits = []
    ts = [threading.Thread(target=lambda: waits.append(b.reserve())) for _ in range(40)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sorted(waits) == [0] * 6 + [i / 2 for i in range(1, 35)]


def test_documented_rates():
    assert server.RATE_LIMITS["getInboundPlan"] == (2, 6)
    assert server.RATE_LIMITS["getShipment"] == (2, 6)
    assert server.RATE_LIMITS["getShipmentItemsByShipmentId"] == (2, 30)
    assert server.RATE_LIMITS["getPricing"] == (0.5, 1)
    assert server.RATE_LIMITS["createReport"] == (0.0167, 15)
    assert set(server._BUCKETS) == set(server.RATE_LIMITS)


def test_every_attempt_waits_for_its_operations_bucket(monkeypatch):
    slept = []
    monkeypatch.setattr(server.time, "sleep", slept.append)
    monkeypatch.setitem(server._BUCKETS, "getPricing", server._Bucket(0.5, 1, clock=Clock()))
    for _ in range(3):
        server._with_retry(lambda: "ok", op="getPricing")
    assert slept == [2.0, 4.0]


# --- 429: backoff and retry, never fatal -----------------------------------------

def _raiser(codes):
    from sp_api.base.exceptions import get_exception_for_code
    it = iter(codes)

    def f():
        c = next(it)
        if c is None:
            return "ok"
        raise get_exception_for_code(c)([{"message": "x"}])
    return f


def test_429_is_retried_past_max_attempts_and_drains_the_bucket(monkeypatch):
    clk = Clock()
    b = server._Bucket(2, 6, clock=clk)
    monkeypatch.setitem(server._BUCKETS, "getShipment", b)
    assert server._with_retry(_raiser([429] * 7 + [None]), max_attempts=2, op="getShipment") == "ok"
    assert b.tokens < 0  # drained: concurrent callers back off too


def test_429_gives_up_only_after_the_throttle_budget():
    from sp_api.base.exceptions import SellingApiException
    with pytest.raises(SellingApiException):
        server._with_retry(_raiser([429] * 50), op="getShipment")


def test_5xx_still_stops_at_max_attempts():
    from sp_api.base.exceptions import SellingApiException
    with pytest.raises(SellingApiException):
        server._with_retry(_raiser([503, 503, None]), max_attempts=2)


async def test_inbound_plans_ride_out_a_burst_of_429s(api):
    walk_fixture.install(api)
    hits = {"n": 0}
    real = api.routes[-1].side_effect

    def flaky(request):
        if request.url.path.endswith("/inboundPlans/PG") and hits["n"] < 7:
            hits["n"] += 1
            return THROTTLED
        return real(request)

    api.routes[-1].side_effect = flaky
    res = await call("get_inbound_plans")
    assert hits["n"] == 7 and "error" not in res
    assert res["plans"] == old("get_inbound_plans", {})["plans"]


# --- same output as before, in the same order ------------------------------------

async def test_inbound_plans_match_the_old_walk(api):
    walk_fixture.install(api)
    for args in ({}, {"statuses": "ACTIVE,SHIPPED", "days": 120}):
        res = await call("get_inbound_plans", **args)
        want = old("get_inbound_plans", args)
        assert res["plans"] == want["plans"] and res["skipped"] == want["skipped"]
        assert (want["pages"], res["pages"]) == (6, 5)  # SHIPPED stops at the old plan's page


async def test_inbound_plans_by_id_match_the_old_walk(api):
    walk_fixture.install(api)
    args = {"plan_ids": "PG,PB,PH"}
    assert await call("get_inbound_plans", **args) == old("get_inbound_plans", args)


async def test_pickups_match_the_old_walk(api):
    walk_fixture.install(api)
    for args in ({}, {"fcs": "ISK3,BLR4,DED5,NAX1",
                      "statuses": "WORKING,READY_TO_SHIP,IN_TRANSIT,SHIPPED,CANCELLED"}):
        assert await call("get_confirmed_inbound_pickups", **args) == \
            old("get_confirmed_inbound_pickups", args)


def _slow_early(path):
    """The first-listed plans answer last, so a walk that kept completion order would fail."""
    for i, pid in enumerate(("PA", "PB", "PC", "PD", "PE")):
        if path.endswith(f"/inboundPlans/{pid}") or f"/inboundPlans/{pid}/shipments/" in path:
            REAL_SLEEP(0.05 * (5 - i))


async def test_reads_run_concurrently_and_keep_listing_order(api):
    walk_fixture.install(api, delay=_slow_early)
    t0 = time.monotonic()
    res = await call("get_inbound_plans")
    assert res["plans"] == old("get_inbound_plans", {})["plans"]
    assert time.monotonic() - t0 < 0.75 * sum(0.05 * k for k in range(1, 6)) * 2

    picks = await call("get_confirmed_inbound_pickups", fcs="ISK3,BLR4,DED5,NAX1",
                       statuses="WORKING,READY_TO_SHIP,IN_TRANSIT,SHIPPED,CANCELLED")
    assert picks == old("get_confirmed_inbound_pickups",
                        {"fcs": "ISK3,BLR4,DED5,NAX1",
                         "statuses": "WORKING,READY_TO_SHIP,IN_TRANSIT,SHIPPED,CANCELLED"})


async def test_two_plan_reads_are_in_flight_together(api):
    barrier = threading.Barrier(2, timeout=5)

    def meet(path):
        if path.endswith("/inboundPlans/PA") or path.endswith("/inboundPlans/PB"):
            barrier.wait()  # deadlocks (BrokenBarrierError) if the reads are serial

    walk_fixture.install(api, delay=meet)
    res = await call("get_inbound_plans", statuses="ACTIVE")
    assert "error" not in res


# --- listing: newest update first, stop at the window ----------------------------

async def test_listing_asks_for_newest_update_first(api):
    walk_fixture.install(api)
    await call("get_inbound_plans")
    reqs = [c.request for c in api.routes[-1].calls if c.request.url.path.endswith("/inboundPlans")]
    assert reqs and all(r.url.params["sortBy"] == "LAST_UPDATED_TIME" and
                        r.url.params["sortOrder"] == "DESC" for r in reqs)


async def test_listing_without_update_times_reads_every_page(api):
    IB = "https://sellingpartnerapi-eu.amazon.com/inbound/fba/2024-03-20"
    route = api.get(f"{IB}/inboundPlans").mock(side_effect=[
        httpx.Response(200, json={"inboundPlans": [{"inboundPlanId": "OLD", "createdAt": "2001-01-01T00:00:00Z"}],
                                  "pagination": {"nextToken": "N"}}),
        httpx.Response(200, json={"inboundPlans": []})])
    res = await call("get_inbound_plans", statuses="ACTIVE")
    assert route.call_count == 2 and res["plans"] == [] and res["pages"] == 2


# --- terminal shipments are read once --------------------------------------------

async def test_terminal_shipments_are_not_read_again(api):
    calls = []
    walk_fixture.install(api, calls=calls)
    first = await call("get_inbound_plans")
    n = len(calls)
    second = await call("get_inbound_plans")
    again = calls[n:]
    assert first == second
    for sid in ("SD1", "SH1", "SJ2"):  # CANCELLED, CLOSED, CLOSED
        assert not paths(again, f"/shipments/{sid}")
    for sid in ("SA1", "SB1", "SG1", "SH2"):  # still moving: always re-read
        assert paths(again, f"/shipments/{sid}")


async def test_pickups_reuse_a_cancelled_shipments_option(api):
    calls = []
    walk_fixture.install(api, calls=calls)
    args = {"fcs": "ISK3,BLR4,DED5,NAX1", "statuses": "WORKING,READY_TO_SHIP,CANCELLED"}
    first = await call("get_confirmed_inbound_pickups", **args)
    n = len(calls)
    assert await call("get_confirmed_inbound_pickups", **args) == first
    again = [p for p in calls[n:] if "/inboundPlans/PD/" in p]
    assert again == []
    assert any("/inboundPlans/PA/" in p for p in calls[n:])


# --- get_inbound_by_fc: shipped - received, WORKING excluded (inbound.py's rules) ---

INBOUND_PY_STATUSES = ("READY_TO_SHIP", "SHIPPED", "IN_TRANSIT", "DELIVERED", "CHECKED_IN",
                       "RECEIVING")  # fba-planner ship_agent/config.py IN_TRANSIT_STATUSES


async def test_by_fc_counts_what_inbound_py_counts(api):
    walk_fixture.install(api)
    want = {}
    plans = (await call("get_inbound_plans"))["plans"]
    for p in plans:  # ship_agent/inbound.py: _collect + in_transit, with raw FC codes
        for s in p["shipments"]:
            if s["status"] not in INBOUND_PY_STATUSES:
                continue
            got = await call("get_inbound_shipment_items", plan_id=p["plan_id"],
                             shipment_id=s["shipment_id"], confirmation_id=s["confirmation_id"] or "")
            for i in got["items"]:
                n = max(0, i["shipped"] - i["received"])
                if n:
                    want[(s["destination"], i["msku"])] = want.get((s["destination"], i["msku"]), 0) + n
    res = await call("get_inbound_by_fc")
    assert {(r["fc"], r["sku"]): r["qty"] for r in res["rows"]} == want
    assert res["total"] == sum(want.values()) == 50
    assert ("ISK3", "M1") not in want  # the WORKING shipment
    assert res["plans"] == 10 and res["skipped_awd_plans"] == 1 and res["shipments"] == 7


async def test_by_fc_partial_receipt_and_over_receipt(api):
    walk_fixture.install(api)
    rows = {(r["fc"], r["sku"]): r["qty"] for r in (await call("get_inbound_by_fc"))["rows"]}
    # SG1 RECEIVING: M1 10 shipped / 4 received -> 6 (+6 from SB1); M2 3/5 -> 0 (+4 from SB1)
    assert rows[("BLR4", "M1")] == 12 and rows[("BLR4", "M2")] == 4


async def test_by_fc_old_count_differs_only_by_the_fix(api):
    walk_fixture.install(api)
    res = await call("get_inbound_by_fc")
    was = old("get_inbound_by_fc", {})
    assert was["total"] == 67 and res["total"] == 50  # 11 WORKING + 6 received units, not in transit
    assert set(res) == set(was)


async def test_by_fc_explicit_working_is_still_honoured(api):
    walk_fixture.install(api)
    res = await call("get_inbound_by_fc", statuses="WORKING")
    assert res["rows"] == [{"fc": "ISK3", "sku": "M1", "qty": 10}]
