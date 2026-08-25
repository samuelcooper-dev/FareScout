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
| Live ingestion | Python (synthetic generator) | Realistic price simulation based on historical patterns, seasonality, booking windows |
| Orchestration | Airflow | Retries and failure isolation across two independently-paced sources |
| Transform | dbt | Staging → intermediate → marts, tested and documented |
| Event queue | Redis / BigQuery table | Decouples price-drop detection from alert delivery |
| Warehouse | BigQuery | Standing free-tier allowance, partitioned tables, 15.7M+ rows |
| Backend | FastAPI | Async-friendly for warehouse-backed endpoints |
| Frontend | React + Vite | Route search, price history, deal score display |
| Deployment | Local (dev) / Render + Vercel (prod) | Free tiers, demonstrates full deployment pipeline |

## Architecture

```
DOT historical fares (bulk)        Synthetic price generator (scheduled)
        ↓                                   ↓
   Ingestion layer (src/ingestion/) — independent failure isolation
        ↓                                   ↓
   BigQuery raw.fares_historical    BigQuery raw.price_snapshots
        ↓                                   ↓
        └────────────┬──────────────────────┘
                     ↓
   dbt: staging → intermediate (route baselines) → marts (deal scores)
                     ↓
              BigQuery analytics
                     ↓
              price_drop_check → event queue → alert_worker → alerts table
                     ↓
              FastAPI endpoints
                     ↓
              React frontend
```

## Key Technical Decisions

**Why synthetic price data instead of a live flight API?** Amadeus Self-Service API (the primary free option) shut down in July 2026. Rather than abandon the project or pay for enterprise API access, the pipeline uses a synthetic price generator that produces realistic variations based on historical patterns (seasonality, booking windows, day-of-week trends). This demonstrates the complete data engineering architecture — ETL, transformations, orchestration, event-driven patterns — without external API dependency. The poller module is designed to be API-agnostic; swapping synthetic data for a real API client is a single-file change.

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

### Phase 1: Core Data Transformations ✅ Complete
- [x] DOT historical bulk load (15.7M rows, 2025 Q1-Q2)
- [x] BigQuery infrastructure setup (datasets, tables, partitioning)
- [x] dbt staging → intermediate → marts models
- [x] dbt tests and documentation
- [x] Deal-score validation against historical baselines

### Phase 2: Synthetic Live Data Pipeline ✅ Complete
- [x] Price simulator (realistic variance based on historical patterns)
- [x] Synthetic snapshot generation (seasonality, booking window, day-of-week)
- [x] Load synthetic snapshots to `raw.price_snapshots`
- [x] mart_deal_scores end-to-end pipeline working
- [ ] Incremental dbt models for streaming data (Phase 5 optimization)

### Phase 3: Production Orchestration
- [ ] Airflow local setup
- [ ] DAG 1: Historical data refresh (monthly)
- [ ] DAG 2: Synthetic polling (every 6 hours)
- [ ] DAG 3: dbt model runs (triggered after new data)
- [ ] DAG 4: Price-drop event detection
- [ ] Retry logic, failure handling, SLAs

### Phase 4: Event-Driven Architecture
- [ ] Price-drop detection logic (% below baseline threshold)
- [ ] Event queue (Redis or BigQuery table)
- [ ] Alert worker (consumes events, writes to alerts table)
- [ ] Decoupled producer/consumer pattern

### Phase 5: Analytics & Optimization
- [ ] Performance tuning (partitioning, clustering, incremental models)
- [ ] Data quality monitoring and anomaly detection
- [ ] FastAPI backend with deal-score endpoints
- [ ] React search UI with price history visualization
- [ ] Published metrics dashboard

## Technical Notes

**Live Pricing API:** Originally designed to use Amadeus Self-Service API, which shut down in July 2026. The project now uses a synthetic price generator that produces realistic price variations based on historical patterns (seasonality, booking windows, day-of-week trends). This demonstrates the full data engineering pipeline without API dependency, while keeping the architecture identical to what a production system would use — swapping the synthetic poller for a real API client is a single-file change.

**Data Scope:** Currently loaded 2025 Q1-Q2 (15.7M rows). Q3-Q4 not yet released by DOT. The baseline logic works with 6 months of data; additional quarters will improve seasonal accuracy.

## Future Enhancements

- **More granular baselines** — split by cabin class, day-of-week, and carrier in addition to season/lead-time
- **Incremental processing optimization** — partition pruning and clustering strategies for multi-year datasets
- **Real-time streaming** — Kafka/Pub-Sub integration if migrating to true live API
- **Historical backtesting framework** — replay past price data against scoring logic to validate accuracy
- **Multi-channel alerting** — SMS/push notifications via Twilio or Firebase
- **ML price forecasting** — ARIMA or Prophet models to predict future price movements beyond baseline comparison
