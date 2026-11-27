"""A synthetic SP-API inbound account, served by URL (not call order) so concurrent reads see
the same answers as serial ones. Shaped like a real daily walk: two listing statuses, pages,
plans older than the cutoff, an AWD plan, shipments in every status the tools care about, v0
items with partial receipts, paginated v2024 items and transportation options. All ids and
SKUs are made up."""

import httpx

IB = "/inbound/fba/2024-03-20"
PAGE = 3  # plans per listing page (the real page is 30)

# (plan_id, listed under, createdAt, lastUpdatedAt, [(shipment_id, listed status, getShipment
# status, destination, [(msku, shipped, received)] or None for no confirmation id)])
PLANS = [
    ("PA", "ACTIVE", "2099-03-01T10:00:00Z", "2099-03-09T10:00:00Z",
     [("SA1", "WORKING", "WORKING", "ISK3", [("M1", 10, 0)])]),
    ("PB", "ACTIVE", "2099-03-02T10:00:00Z", "2099-03-08T10:00:00Z",
     [("SB1", "READY_TO_SHIP", "READY_TO_SHIP", "BLR4", [("M1", 6, 0), ("M2", 4, 0)]),
      ("SB2", "READY_TO_SHIP", "IN_TRANSIT", "DED3", [("M3", 8, 0)])]),
    ("PC", "ACTIVE", "2099-02-20T10:00:00Z", "2099-03-07T10:00:00Z", []),
    ("AWD", "ACTIVE", "2099-02-19T10:00:00Z", "2099-03-06T10:00:00Z", None),
    ("PD", "ACTIVE", "2099-02-18T10:00:00Z", "2099-03-05T10:00:00Z",
     [("SD1", "CANCELLED", "CANCELLED", "NAX1", [("M4", 5, 0)])]),
    ("PE", "ACTIVE", "2099-02-10T10:00:00Z", "2099-03-04T10:00:00Z",
     [("SE1", "WORKING", "READY_TO_SHIP", "DED5", None)]),
    ("PF", "ACTIVE", "2001-01-01T10:00:00Z", "2001-02-01T10:00:00Z",
     [("SF1", "CLOSED", "CLOSED", "ISK3", [("M1", 1, 1)])]),
    ("PB", "SHIPPED", "2099-03-02T10:00:00Z", "2099-03-08T10:00:00Z", "dup"),
    ("PG", "SHIPPED", "2099-01-20T10:00:00Z", "2099-03-03T10:00:00Z",
     [("SG1", "RECEIVING", "RECEIVING", "BLR4", [("M1", 10, 4), ("M2", 3, 5), ("M5", 7, 0)])]),
    ("PH", "SHIPPED", "2099-01-15T10:00:00Z", "2099-03-02T10:00:00Z",
     [("SH1", "CLOSED", "CLOSED", "DED5", [("M1", 9, 9)]),
      ("SH2", "IN_TRANSIT", "IN_TRANSIT", "DED5", [("M2", 12, 0)])]),
    ("PI", "SHIPPED", "2099-01-10T10:00:00Z", "2099-03-01T10:00:00Z",
     [("SI1", "DELIVERED", "DELIVERED", "ISK3", [("M3", 2, 0)])]),
    ("PJ", "SHIPPED", "2099-01-05T10:00:00Z", "2099-02-28T10:00:00Z",
     [("SJ1", "CHECKED_IN", "CHECKED_IN", "NAX1", [("M6", 4, 0)]),
      ("SJ2", "CLOSED", "CLOSED", "NAX1", [("M6", 3, 3)])]),
    ("PK", "SHIPPED", "2001-01-01T10:00:00Z", "2001-01-20T10:00:00Z",
     [("SK1", "CLOSED", "CLOSED", "BLR4", [("M1", 2, 2)])]),
    ("PL", "SHIPPED", "2000-12-01T10:00:00Z", "2001-01-10T10:00:00Z",
     [("SL1", "CLOSED", "CLOSED", "BLR4", [("M1", 2, 2)])]),
]


def _plans():
    out = {}
    for pid, _, created, updated, ships in PLANS:
        if ships != "dup":
            out[pid] = (created, updated, ships)
    return out


def _listing(status):
    """Plans listed under ``status`` (no status: all of them, one row per plan), newest update
    first, as the live listing returns them."""
    rows = [p for p in PLANS if status in (None, p[1]) and (status or p[4] != "dup")]
    rows.sort(key=lambda p: p[3], reverse=True)
    return [{"inboundPlanId": pid, "createdAt": c, "lastUpdatedAt": u, "status": status}
            for pid, _, c, u, _s in rows]


def _ok(js):
    return httpx.Response(200, json=js)


def install(router, *, delay=None, calls=None):
    """Mount the account on a respx router. ``delay(path)`` may sleep (real time) to make some
    answers slow; ``calls`` collects request paths (with query) in arrival order."""
    plans = _plans()

    def handler(request):
        path, qp = request.url.path, request.url.params
        if calls is not None:
            calls.append(path)
        if delay:
            delay(path)
        rest = path[len(IB):].strip("/").split("/") if path.startswith(IB) else None
        if rest == ["inboundPlans"]:
            # The token carries the listing's status, so a page sent with only the token (as the
            # pickups tool does) continues the same listing.
            tok = qp.get("paginationToken")
            status, start = tok.split(":") if tok else (qp.get("status") or "", "0")
            rows, start = _listing(status or None), int(start)
            body = {"inboundPlans": rows[start:start + PAGE]}
            if start + PAGE < len(rows):
                body["pagination"] = {"nextToken": f"{status}:{start + PAGE}"}
            return _ok(body)
        if rest and len(rest) == 2:
            created, updated, ships = plans[rest[1]]
            if ships is None:
                return httpx.Response(400, json={"errors": [{"code": "BadRequest", "message":
                    "ERROR: Operation GetInboundPlan is not supported for Amazon Warehousing "
                    "and Distribution inbound plans."}]})
            return _ok({"inboundPlanId": rest[1], "status": "ACTIVE", "createdAt": created,
                        "lastUpdatedAt": updated,
                        "shipments": [{"shipmentId": s[0], "status": s[1]} for s in ships]})
        if rest and len(rest) == 3 and rest[2] == "transportationOptions":
            sid = qp["shipmentId"]
            opts = [{"transportationOptionId": f"OTHER-{sid}"},
                    {"transportationOptionId": f"TO-{sid}", "shippingSolution": "AMAZON_PARTNERED_CARRIER",
                     "carrier": {"alphaCode": "ATSP"},
                     "carrierAppointment": {"startTime": "2099-03-10T04:30:00Z",
                                            "endTime": "2099-03-10T07:45:00Z"},
                     "quote": {"voidableUntil": "2099-03-09T00:00:00Z",
                               "cost": {"amount": 450.0, "code": "INR"}}}]
            if qp.get("paginationToken"):
                return _ok({"transportationOptions": opts[1:]})
            return _ok({"transportationOptions": opts[:1], "pagination": {"nextToken": "p2"}})
        if rest and len(rest) >= 4:
            ships = plans[rest[1]][2]
            sid, listed, status, dest, items = next(s for s in ships if s[0] == rest[3])
            if len(rest) == 4:
                full = {"shipmentId": sid, "status": status, "destination": {"warehouseId": dest},
                        "selectedTransportationOptionId": f"TO-{sid}"}
                if items is not None:
                    full["shipmentConfirmationId"] = f"FBA{sid}"
                return _ok(full)
            if rest[4] == "items":  # v2024: shipped only, two pages when there are 2+ items
                lines = [{"msku": m, "quantity": q} for m, q, _ in items or [("MX", 1, 0)]]
                if qp.get("paginationToken"):
                    return _ok({"items": lines[1:]})
                body = {"items": lines[:1]}
                if len(lines) > 1:
                    body["pagination"] = {"nextToken": "more"}
                return _ok(body)
        if path.startswith("/fba/inbound/v0/shipments/"):
            conf = path.split("/")[5]
            for _c, _u, ships in plans.values():
                for s in ships or []:
                    if f"FBA{s[0]}" == conf:
                        return _ok({"payload": {"ItemData": [
                            {"SellerSKU": m, "QuantityShipped": q, "QuantityReceived": r}
                            for m, q, r in s[4]]}})
        raise AssertionError(f"unmocked {request.url}")

    router.route(host="sellingpartnerapi-eu.amazon.com").mock(side_effect=handler)
