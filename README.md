# FareScout — Flight Price Intelligence Pipeline

Flight search sites show you a number. They don't tell you whether it's actually a good number. FareScout tracks fares over time and scores every live price against what that route historically costs — so you know if $340 to Chicago is a deal or just a Tuesday.

**Status: in active development.** This README describes the target architecture and is being built out phase by phase — see Roadmap below for what's live vs. planned.

## The Problem

Airfare search tools are stateless — they show you today's price and nothing else. There's no memory of what a route "normally" costs, no way to tell a genuine deal from typical seasonal pricing, and no alerting when a price actually drops. Travelers either obsessively refresh a search or rely on third-party trackers that don't explain their reasoning.

## What FareScout Does

- **Live price tracking** — a watchlist of routes polled on a schedule against the Amadeus flight-offers API, building a price-over-time history per route
- **Historical baselines** — years of DOT On-Time Performance / fare data loaded and modeled to establish what a route typically costs by season and booking lead time
- **Deal scoring** — every live snapshot compared against its route's historical baseline, so "cheap" means something quantified, not just a gut call
- **Price-drop alerts** — an event-driven check that fires when a live price crosses a threshold below baseline, decoupled from the polling pipeline via a queue
- **Simple search UI** — look up a route, see the current price, its deal score, and the price history behind it

## How It Works — From Ingestion to Alert

1. **Historical load**

   DOT fare/on-time data is loaded in bulk, giving the pipeline years of route pricing to establish a real baseline — not a guess.

2. **Scheduled polling**

   Every few hours, FareScout polls Amadeus for a fixed watchlist of routes and writes a raw price snapshot for each. This is a deliberate scheduled-polling design, not a claim of true real-time streaming — flight pricing doesn't expose a public live feed the way flight *position* data does.

3. **Transform (dbt)**

   Raw historical fares and raw snapshots are modeled through staging → intermediate → marts layers: route-level baselines by season and lead time, then a deal-score mart comparing each live snapshot against its baseline.

4. **Event check**

   Each new snapshot is checked against the route's baseline. A drop past the threshold publishes an event to a queue, decoupled from the poller itself, so a slow or failing alert consumer never blocks ingestion.

5. **Search & alerts**

   The API and frontend read directly from the marts — search a route, see the current price and deal score, or check the alerts feed for recent drops.

## Stack

| Layer | Tech | Why |
|---|---|---|
| Batch ingestion | Python | DOT historical fare/on-time bulk load |
| Live ingestion | Python + Amadeus API | Scheduled route-price polling, not a live feed that doesn't exist publicly |
| Orchestration | Airflow | Retries and failure isolation across two independently-paced sources |
| Transform | dbt | Staging → intermediate → marts, tested and documented |
| Event queue | Redpanda / AWS SQS (free tier) | Decouples price-drop detection from alert delivery |
| Warehouse | BigQuery | Standing free-tier allowance, no trial-credit expiration risk for a long-running pipeline |
| Backend | FastAPI | Async-friendly for warehouse-backed endpoints |
| Frontend | React + Vite | Route search, price history, deal score display |
| Deployment | Render (API) + Vercel (frontend) | Free tiers, auto-deploy on push |

## Architecture

```
DOT historical fares (bulk)        Amadeus flight offers (scheduled poll)
        ↓                                   ↓
   Ingestion layer (src/ingestion/) — one source failing doesn't block the other
        ↓                                   ↓
   Raw staging tables (BigQuery `raw`)      → price-drop check → queue → alert_worker → alerts table
        ↓
   dbt: staging → intermediate (route baselines) → marts (deal scores)
        ↓
   BigQuery analytics dataset
        ↓
   FastAPI  →  React frontend
```

## Key Technical Decisions

**Why scheduled polling instead of calling it "real-time"?** Flight pricing has no public live-feed API the way aircraft position data does (that's a fundamentally different data source with its own tracking infrastructure). Being upfront about the scheduled-poll design — snapshot every few hours, not every second — keeps the architecture honest and is still a legitimate, widely-used pattern for price tracking.

**Why decouple alerting with a queue instead of alerting inline in the poller?** A slow email/notification step shouldn't block or slow down the next scheduled poll. Publishing an event and letting a separate consumer handle delivery keeps ingestion reliable even if the alert path is degraded.

**Why BigQuery over Snowflake or Redshift?** Both alternatives are trial-credit based (30 and 90 days respectively) and this pipeline is designed to run continuously for weeks accumulating price history — a warehouse with a standing free allowance, not an expiring credit, is the right fit here.

**Why dbt for transforms instead of hand-written SQL scripts?** Tested, documented, layered models (staging/intermediate/marts) make the baseline and deal-score logic auditable and changeable without breaking downstream tables — important once the deal score is the actual product feature, not just plumbing.

## Local Setup

```bash
# 1. Clone and configure
cp .env.example .env
# Fill in: DATABASE_URL / BQ project credentials, AMADEUS_API_KEY, AMADEUS_API_SECRET, QUEUE_URL

# 2. Load historical data
python -m src.ingestion.dot_fares --load

# 3. Run the poller once (or let Airflow schedule it)
python -m src.ingestion.amadeus_poller

# 4. Build dbt models
cd dbt && dbt run && dbt test

# 5. Start the API
uvicorn src.api.main:app --reload
# → http://localhost:8000

# 6. Start the frontend
cd frontend && npm install && npm run dev
# → http://localhost:5173
```

## Roadmap

- [ ] Amadeus polling pipeline + raw snapshot storage
- [ ] DOT historical bulk load at real scale (multi-year)
- [ ] dbt staging → intermediate → marts, with tests
- [ ] Airflow DAGs replacing ad hoc scheduling, with retries/failure isolation
- [ ] Price-drop event pipeline (queue + alert worker)
- [ ] FastAPI + React search UI with deal scores and price history
- [ ] Published metrics: rows loaded, pipeline runtime, alert lead time

## What I'd Do Next

- **More granular baselines** — split baseline by cabin class and day-of-week, not just season and lead time, once there's enough snapshot volume to support it
- **Multi-route expansion** — the watchlist starts small (5–10 routes) to stay within Amadeus rate limits; the polling design is already route-agnostic, so scaling out is a config change, not a rewrite
- **Historical accuracy backtesting** — replay past DOT data against the deal-score logic to validate that flagged "deals" actually were, before trusting it on live data
- **SMS/push alerts** — the alert pipeline already writes to a table; adding a delivery channel beyond in-app is an additive change to `alert_worker.py`, not a redesign
