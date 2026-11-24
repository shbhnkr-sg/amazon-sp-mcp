# get_orders + inventory-ledger Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two confirmed regressions in the deployed `amazon-sp` MCP server — `get_orders` rejecting every default call, and `get_inventory_ledger` returning 0 rows on its default window.

**Architecture:** Two independent, surgical edits to `server.py`. (A) Stop `get_orders` from sending `CreatedBefore = now` (Amazon rejects a `CreatedBefore` newer than ~2 min). (B) Make `get_inventory_ledger` request `reportOptions.aggregatedByTimePeriod = DAILY` so a rolling day-window yields rows instead of relying on the report's MONTHLY default. Tasks 1 and 2 touch different functions and can run in parallel; Task 3 deploys + verifies both.

**Tech Stack:** Python 3.12, FastMCP, `python-amazon-sp-api`, Docker (server.py baked into the image), deployed on `mk@192.168.0.150:3100`.

## Global Constraints

- Marketplace: `A21TJRUUN4KGV` (Amazon.in); "yesterday"/day boundaries are IST; `PurchaseDate` is UTC.
- `server.py` is **baked into the image** (`Dockerfile:13 COPY server.py`) — every code change requires a rebuild on the host, not just a restart.
- No test framework and credentials live only on the host → verification is **live MCP tool calls against the running server** (before = reproduce the bug, after = confirm the fix).
- Match existing style: tools are `@mcp.tool()` over `@tool_safe`; helpers `_window`/`_paginate`/`_request_and_fetch_report` are shared — **do not** change shared helpers to fix a single tool unless the fix belongs there.
- Pinned baseline for review: deployed `/app/server.py` md5 `96be42aa8dae444f24fd7ca8acdef266`.

## Root Cause Summary (already investigated — evidence captured)

- **get_orders:** `_window` (`server.py:156`) defaults `end = now_local`; `get_orders` (`server.py:295`) sends `CreatedBefore: cb` unconditionally. Amazon's `getOrders` rejects `CreatedBefore` within ~2 min of now → every default call returns `InvalidInput`. Proven: the identical call with an explicit *past* `created_before` succeeds.
- **get_inventory_ledger:** `GET_LEDGER_SUMMARY_VIEW_DATA` defaults to `aggregatedByTimePeriod = MONTHLY` and only emits a row for a **complete calendar month** in the window. The tool's default `days_ago=30` is a rolling window (e.g. 28 May→27 Jun) containing no whole calendar month → 0 rows. Proven: window `2026-05-01→2026-06-01` returned a row dated `"05/2026"`; window `2026-06-01→2026-06-21` returned 0 rows.

## File Structure

- Modify: `server.py` — `get_orders` (~line 294-299) and `get_inventory_ledger` (~line 518-525). No new files.

---

### Task 1: Fix `get_orders` default window (no spurious `CreatedBefore`)

**Files:**
- Modify: `server.py` — `get_orders`, the `else` branch that builds the first-page `kwargs` (currently ~line 294-299).

**Interfaces:**
- Consumes: existing `created_before` parameter (raw, in scope) and `cb` from `_window`.
- Produces: no signature change. Same `{orders, count, truncated, next_token}` return shape.

- [ ] **Step 1: Reproduce the failure (the "before" test)**

Call the MCP tool with defaults:
```
get_orders()        # days_ago=7, no created_before
```
Expected NOW: `{"error": {"type": "SellingApiBadRequestException", "code": 400, ... "InvalidInput" ...}}`

- [ ] **Step 2: Apply the fix**

Replace the `else` block in `get_orders`:
```python
        else:
            kwargs = {"CreatedAfter": ca, "CreatedBefore": cb, "MarketplaceIds": [MARKETPLACE]}
            if statuses:
                kwargs["OrderStatuses"] = statuses
            if channels:
                kwargs["FulfillmentChannels"] = channels
```
with:
```python
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
```

Also append a one-line caveat inside the `get_orders` docstring, right before the closing `"""` (after "...PurchaseDate values are UTC."):
```
    A created_before within ~2 min of now is rejected by Amazon; pass an older
    bound or omit it (omitting returns orders up to now).
```

- [ ] **Step 3: Confirm the fix after deploy (the "after" test — runs in Task 3)**

After Task 3 deploys, call:
```
get_orders()                                  # default path
get_orders(created_before="2026-06-27")       # explicit upper bound still honored
```
Expected: first returns `{"orders": [...], "count": >0, ...}` (no error); second returns only orders before 27 Jun IST.

- [ ] **Step 4: Commit (only if the repo is under version control; else stage for deploy in Task 3)**

```bash
git add server.py && git commit -m "fix(get_orders): don't send CreatedBefore=now (Amazon rejects <2min-old upper bound)"
```

---

### Task 2: Fix `get_inventory_ledger` empty default (request DAILY aggregation)

**Files:**
- Modify: `server.py` — `get_inventory_ledger` (currently ~line 518-525).

**Interfaces:**
- Consumes: existing `_request_and_fetch_report(..., extra_options=...)` — it merges `extra_options` into the `create_report` body (`server.py:437-438`).
- Produces: new optional param `aggregated_by: str = "DAILY"`. Same TSV return envelope.

- [ ] **Step 1: VERIFY the exact `reportOptions` schema before coding**

The key casing is asymmetric and easy to get wrong. Confirm via the SP-API reference (use `mcp__sp-api-dev-assistant__sp_api_reference` for Reports / report type `GET_LEDGER_SUMMARY_VIEW_DATA`, or read the installed library) that the option keys/values are exactly:
- `aggregateByLocation`: `"COUNTRY"` | `"FC"`
- `aggregatedByTimePeriod`: `"DAILY"` | `"MONTHLY"` (note: *aggregate*ByLocation vs *aggregated*ByTimePeriod)

Also confirm `Reports.create_report(**opts)` forwards a `reportOptions=` kwarg into the request body. If either differs, stop and adjust the literals below to match.

- [ ] **Step 2: Reproduce the failure (the "before" test)**

```
get_inventory_ledger()        # days_ago=30, rolling window
```
Expected NOW: `{"format": "tsv", "rows": 0, ...}` (header only).

- [ ] **Step 3: Apply the fix**

Replace `get_inventory_ledger`:
```python
@mcp.tool()
@tool_safe
def get_inventory_ledger(days_ago: int = 30, after: str = None, before: str = None,
                         max_rows: int = 500, fields: str = None, report_id: str = None) -> dict:
    """Get inventory ledger report — all inventory movements (receipts, shipments,
    removals, adjustments) for the last N days or an IST date range."""
    return _request_and_fetch_report("GET_LEDGER_SUMMARY_VIEW_DATA", days_ago, after, before,
                                     max_rows=max_rows, fields=fields, report_id=report_id)
```
with:
```python
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

> Pre-verified live (10-day window) before deploy: `aggregateByLocation:"FC"` + `aggregatedByTimePeriod:"DAILY"` returned rows with daily `MM/DD/YYYY` dates and 13 distinct FC codes — DAILY + FC both confirmed against the real account.
```

- [ ] **Step 4: Confirm the fix after deploy (the "after" test — runs in Task 3)**

After Task 3 deploys, run two checks:

(4a) Pre-flight the DAILY assumption on a small window:
```
get_inventory_ledger(days_ago=7, max_rows=5)
```
Expected: `rows > 0` with **≥2 distinct `Date` values** (daily buckets; the column is `MM/DD/YYYY`, since the monthly default emitted `MM/YYYY` like `"05/2026"`), not a single monthly bucket. Do NOT assert an exact date string — assert only rows>0 and multiple distinct dates. If `{"status":"IN_PROGRESS", reportId}`, re-call with that `report_id` until DONE.

(4b) Verify the actual default the tool serves (30-day, ~544 SKUs ≈ up to ~16k rows):
```
get_inventory_ledger()
```
Expected: `rows > 0` **OR** `{"status":"IN_PROGRESS", reportId}` — for a report this large, IN_PROGRESS is a normal outcome (it exceeds the ~40s poll in `_poll_report`), not a failure. On IN_PROGRESS, re-call with `report_id` until DONE, then confirm `rows > 0`.

Failure → return to systematic-debugging Phase 1 (do not stack another guess). Failure is ONLY: `rows == 0` while status is DONE, or `{"error": ... "FATAL"}` (would mean a DAILY 30-day range was rejected — a new failure mode replacing today's instant-but-empty result).

- [ ] **Step 5: Commit (or stage for deploy in Task 3)**

```bash
git add server.py && git commit -m "fix(get_inventory_ledger): request DAILY aggregation so rolling windows return rows"
```

---

### Task 3: Deploy and verify both fixes live

**Files:** none (deploy + verification only).

**⚠️ This restarts the production MCP server — get explicit user approval before running.**

Host repo path (resolved from the compose project label): `/home/mk/amazon-sp-mcp`. Record the **pre-deploy** deployed md5 first for rollback reference: `ssh mk@192.168.0.150 'docker exec amazon-sp-mcp md5sum /app/server.py'` (baseline is `96be42aa8dae444f24fd7ca8acdef266`).

- [ ] **Step 1: Copy updated server.py to the host**

```bash
scp server.py mk@192.168.0.150:/home/mk/amazon-sp-mcp/server.py
```

- [ ] **Step 2: Rebuild + recreate the container**

```bash
ssh mk@192.168.0.150 'cd /home/mk/amazon-sp-mcp && docker compose up -d --build'
```

- [ ] **Step 3: Confirm health + that the new code is live**

```bash
curl -s http://192.168.0.150:3100/health        # expect: OK
md5 -q server.py                                 # local NEW (post-fix) hash
ssh mk@192.168.0.150 'docker exec amazon-sp-mcp md5sum /app/server.py'
```
Expected: deployed md5 == the **new post-fix local** hash (it will NOT equal the pre-fix baseline `96be4...`; that's the point).

- [ ] **Step 4: Run the "after" tests from Task 1 Step 3 and Task 2 Step 4**

Expected: `get_orders()` → orders (no error) and `get_orders(created_before="2026-06-27")` honors the bound; `get_inventory_ledger(days_ago=7, max_rows=5)` → rows>0 across ≥2 distinct dates (per Task 2 Step 4a) and `get_inventory_ledger()` → rows>0 or IN_PROGRESS→rows>0 (Task 2 Step 4b).

- [ ] **Step 5: Regression sweep — re-run the default path of every tool**

Call each with defaults and assert no `{"error"}` envelope. Acceptable non-error outcomes: `NO_DATA` (genuinely empty reports — reimbursements, LTSF, settlement) and `IN_PROGRESS` (large reports). **Exception:** `get_inventory_ledger` must return `rows>0` (or IN_PROGRESS→rows>0) — `NO_DATA`/0-rows there means the fix regressed. Tools: `get_orders`, `get_inventory_summary`, `get_sales_metrics`, `get_fba_reimbursements`, `get_fba_storage_fees`, `get_fba_long_term_storage_fees`, `get_inventory_ledger`, `get_sales_traffic_report`, `get_settlement_report`.

- [ ] **Step 6: Rollback (only if Step 4/5 fails)**

The change is additive; revert by rebuilding the prior code. Restore the previous `server.py` on the host (git checkout of the prior commit, or re-copy the pre-fix file) and re-run `docker compose up -d --build`; confirm `docker exec amazon-sp-mcp md5sum /app/server.py` returns to `96be42aa8dae444f24fd7ca8acdef266`.

---

## Recommended Follow-up (not in scope here)

- Add a committed `smoke_test.py` that calls every tool's **default-arg** path and asserts no error envelope — the single guard that would have caught the `get_orders` regression before deploy. Deferred because a robust MCP smoke harness is its own small task and shouldn't block these two fixes.
- Consider `_window` defaulting `end` to `now − 2 min` as defense-in-depth, so any *future* tool that sends an upper bound to a recency-sensitive API can't reintroduce this class of bug. Left out here to stay surgical (only `get_orders` needs it today).

## Self-Review

- **Spec coverage:** Issue #1 → Task 1; ledger issue → Task 2; deploy/verify → Task 3. ✓
- **Placeholders:** none — both diffs are complete literal code; `<repo-path>` is the only host-specific value to fill at deploy.
- **Type consistency:** Task 1 adds no params; Task 2 adds `aggregated_by: str = "DAILY"` and passes `extra_options=` (a kwarg `_request_and_fetch_report` already accepts at `server.py:423`). ✓
- **Open assumption (flagged in Task 2 Step 1):** the `reportOptions` key casing (`aggregateByLocation` / `aggregatedByTimePeriod`) was independently confirmed correct against Amazon's `report-type-values-fba` docs, and that `create_report(**opts)` forwards `reportOptions` into the POST body was confirmed in the library source. The remaining unproven point — DAILY actually yields rows at the served 30-day default — is verified at deploy via Task 2 Steps 4a/4b (incl. IN_PROGRESS handling), not assumed.

## Independent review

Plan reviewed by an independent Opus agent against the pinned `96be42aa` snapshot: **APPROVE-WITH-CHANGES**, no blocking code errors. All five non-blocking tightenings (IN_PROGRESS as an expected ledger outcome + follow `report_id`; don't assert an exact date format; explicit rollback step; near-now `created_before` docstring caveat; resolved host path + md5 wording) are incorporated above.
