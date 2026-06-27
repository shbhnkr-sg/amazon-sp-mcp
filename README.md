# Amazon SP-API MCP Server

A single-file MCP server (`server.py`) that wraps
[`python-amazon-sp-api`](https://github.com/saleweaver/python-amazon-sp-api) and
exposes Amazon Seller Central data as MCP tools over **FastMCP's native
Streamable HTTP transport** — no supergateway, no stdio bridge, direct HTTP.

Both **Claude Code** and **Codex** connect to the same URL, from any device
(Windows / macOS / Linux). Read-only: no order cancellation, no listing edits.

- **Marketplace:** Amazon.in (`A21TJRUUN4KGV`, EU endpoint) — pinned server-side
  via `MARKETPLACE_ID`, so **every** client uses the same marketplace. It is not
  a per-client setting and cannot be overridden by a client.
- **Auth:** LWA only (refresh token + client id/secret). No AWS IAM needed.
- **Endpoints:** MCP at `/mcp`, health check at `/health`.

## Tools (14)

| Category | Tools |
|----------|-------|
| Orders | `get_orders`, `get_order`, `get_order_items` |
| Inventory (FBA) | `get_inventory_summary`, `get_inventory_ledger` |
| Sales | `get_sales_metrics`, `get_sales_traffic_report` |
| Finance / fees | `get_settlement_report`, `get_fba_reimbursements`, `get_fba_storage_fees`, `get_fba_long_term_storage_fees`, `get_fee_estimate` |
| Catalog | `get_catalog_item`, `search_catalog` |

Report-backed tools (settlement, reimbursements, storage fees, ledger, traffic)
use the SP-API async Reports flow: request → poll → download.

## Response conventions

All tools are bounded and consistent so responses stay within an agent's token budget:

- **List tools** (`get_orders`, `get_inventory_summary`, `get_order_items`) return
  `{ "<items>": [...], "count", "truncated", "next_token" }`. They paginate
  automatically up to a cap (`max_orders` / `max_items`, ~page-aligned). If
  `truncated` is true, pass `next_token` back in to fetch the next slice.
- **Field projection:** pass `fields="a,b,c"` (comma-separated top-level keys) to
  any list/catalog tool to trim the payload — e.g.
  `get_orders(fields="AmazonOrderId,OrderStatus,OrderTotal,PurchaseDate")`.
- **Time windows:** `get_orders`, `get_sales_metrics`, and report tools accept
  either `days_ago` (last N days) or an explicit IST range via
  `created_after`/`created_before` (orders/sales) or `after`/`before` (reports),
  as ISO dates/datetimes interpreted in **IST**. Order `PurchaseDate` values are
  UTC. Sales windows are floored to IST midnight so day buckets are whole days.
- **Report tools** return `{reportId, rows, truncated, content}` for TSV reports
  (with `max_rows` cap + `fields` column projection) or `{content: <json>}` for the
  sales & traffic report. A report that outlives the gateway timeout returns
  `{status: "IN_PROGRESS", reportId}` — re-call the same tool with `report_id=<id>`
  to fetch it once ready. `get_settlement_report` returns the **latest scheduled**
  settlement report (Amazon generates these; they can't be requested on demand).
- **Errors** are always `{ "error": { "type", "code", "message" } }` — never a raw
  500 — so a throttle, bad input, or not-found is distinguishable.
- **Throttling:** every SP-API call retries 429/5xx with exponential backoff.

## Credentials

Copy `.env.example` to `.env` and fill in:

```
LWA_CLIENT_ID=amzn1.application-oa2-client.XXXX
LWA_CLIENT_SECRET=amzn1.oa2-cs.v1.XXXX
LWA_REFRESH_TOKEN=Atzr|XXXX
MARKETPLACE_ID=A21TJRUUN4KGV
```

## Deploy (on the Linux host)

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f amazon-sp-mcp
```

## Network access

> ⚠️ **This server has no authentication.** Anything that can reach the port can
> read your live Amazon seller data. Choose the exposure that matches how much
> you trust the network.

### Option A — LAN (current default)

Reachable by any device on the local network. Simplest; **exposes the
unauthenticated server to the entire LAN** — use only on a trusted network.

`docker-compose.yml` (already set this way):

```yaml
    ports:
      - "3100:3000"        # binds 0.0.0.0
```

Open the host firewall if one is active, then connect via the host's LAN IP:

```bash
sudo ufw allow 3100/tcp                       # ufw
# or: sudo firewall-cmd --add-port=3100/tcp --permanent && sudo firewall-cmd --reload
curl http://<HOST_LAN_IP>:3100/health         # -> OK
```

Client URL: `http://<HOST_LAN_IP>:3100/mcp`

### Option B — Tailscale (private, recommended)

Only devices on your tailnet can connect; traffic is encrypted; works
identically across Windows/macOS/Linux. Keep the port bound to localhost and let
Tailscale Serve expose it over TLS.

`docker-compose.yml`:

```yaml
    ports:
      - "127.0.0.1:3100:3000"   # localhost-only; not on the LAN
```

On the host:

```bash
tailscale serve --bg 3100
```

Client URL: `https://<HOST>.<TAILNET>.ts.net/mcp`

(Alternative without Serve: bind directly to the host's Tailscale IP with
`"<TAILSCALE_IP>:3100:3000"` and connect to `http://<TAILSCALE_IP>:3100/mcp`.)

## Connect from Claude Code (same on Windows / macOS / Linux)

```bash
claude mcp add --transport http --scope user amazon-sp <CLIENT_URL>
claude mcp get amazon-sp        # should report: ✔ Connected
```

`--scope user` makes it available in every project on that machine. Equivalent
manual config (identical on all OSes):

```json
{
  "mcpServers": {
    "amazon-sp": { "type": "http", "url": "<CLIENT_URL>" }
  }
}
```

## Connect from Codex

Codex has no CLI command for URL-based servers — edit the config file directly:

- macOS / Linux: `~/.codex/config.toml`
- Windows: `%USERPROFILE%\.codex\config.toml`

```toml
[mcp_servers.amazon-sp]
url = "<CLIENT_URL>"
```

Then start a Codex session and run `/mcp` to confirm the tools appear.

## Manage

```bash
docker compose up -d --build   # build/rebuild and start
docker compose ps              # status
docker compose logs -f amazon-sp-mcp
docker compose down            # stop & remove
```
