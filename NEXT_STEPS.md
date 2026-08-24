# FareScout - Next Steps

## Completed ✅

### Phase 1: Core Data Transformations
- ✅ BigQuery setup (project: farescout-506518)
- ✅ Loaded 2025 Q1-Q2 DOT historical fares (15.7M rows)
- ✅ Built dbt models:
  - `stg_fares_historical` - Staging view with unique fare_id
  - `int_route_baselines` - 71,700 route+quarter baselines (median, p25, p75)
- ✅ All dbt tests passing

### Phase 2: Synthetic Live Data Pipeline
- ✅ Created `src/ingestion/price_simulator.py`
- ✅ Generated 200 synthetic price snapshots (10 routes × May 2025)
- ✅ Built `stg_price_snapshots` model
- ✅ Built `mart_deal_scores` - full pipeline end-to-end working
- ✅ Query scripts: `query_baselines.py`, `query_deals.py`

## What to Do Next

### Quick Wins (30-60 min each)

1. **Fix Price Simulator Logic**
   - Current issue: Prices are 7-8x baseline (too high)
   - Location: `src/ingestion/price_simulator.py`, line ~75-85
   - Fix: Reduce multipliers (booking_window_factor, weekend_factor should be 1.0-1.2 range)
   - Test: Re-run simulator, verify deal scores show mix of positive/negative

2. **Add dbt Documentation**
   ```bash
   cd dbt
   ../.venv/Scripts/dbt.exe docs generate
   ../.venv/Scripts/dbt.exe docs serve
   ```
   - Opens interactive lineage graph in browser
   - Shows model dependencies, column descriptions, tests
   - Great for portfolio screenshots

3. **Add More dbt Tests**
   - Edit `dbt/models/intermediate/schema.yml`
   - Add relationship tests (baselines → staging)
   - Add custom tests (median > p25, p75 > median)
   - Add freshness checks on sources

### Phase 3: Airflow Orchestration (2-3 hours)

**Goal:** Automated scheduling and dependency management

**Steps:**
1. Install Airflow locally
   ```bash
   pip install apache-airflow==2.9.0
   airflow db init
   ```

2. Create DAGs (`airflow/dags/`):
   - `dag_historical_refresh.py` - Monthly DOT data refresh
   - `dag_price_polling.py` - Synthetic polling every 6 hours
   - `dag_dbt_transform.py` - Run dbt models after new data
   - `dag_price_alerts.py` - Detect deal events

3. Key features to demonstrate:
   - Task dependencies (dbt waits for data load)
   - Retry logic with exponential backoff
   - SLAs and alerting
   - Parallel task execution
   - Sensor operators (wait for data freshness)

4. Screenshot the Airflow UI showing:
   - DAG graph view
   - Task success/failure states
   - Gantt chart showing parallelism

### Phase 4: Event-Driven Architecture (1-2 hours)

**Goal:** Decouple price-drop detection from alerting

**Steps:**
1. Add price-drop detection logic:
   - Create `src/workers/price_drop_detector.py`
   - Query `mart_deal_scores` for `is_deal = true`
   - Publish events to queue

2. Queue options (pick one):
   - **Redis** (easiest): `pip install redis`, run locally
   - **BigQuery table**: No setup, just INSERT events
   - **Google Pub/Sub**: Cloud-native but needs setup

3. Create alert worker:
   - `src/workers/alert_worker.py`
   - Consumes events from queue
   - Writes to `raw.alerts` table
   - Shows decoupled producer/consumer pattern

4. Update Airflow DAG:
   - Add task to trigger detector after dbt runs
   - Alert worker runs independently

### Phase 5: Analytics & Optimization (2-3 hours)

**Performance tuning:**
1. Add partitioning to dbt models:
   ```sql
   {{ config(
       partition_by={'field': 'polled_at', 'data_type': 'timestamp'},
       cluster_by=['route_origin', 'route_dest']
   ) }}
   ```

2. Convert models to incremental:
   - `stg_price_snapshots` - only process new snapshots
   - `mart_deal_scores` - incremental merge

3. Add dbt snapshots for SCD Type 2:
   - Track baseline changes over time
   - Slowly changing dimension pattern

**Analytics queries:**
1. Best time to book analysis (X days before departure)
2. Route seasonality trends
3. Carrier pricing patterns
4. Deal frequency by route

## Common Commands Reference

### BigQuery
```bash
# View data in console
https://console.cloud.google.com/bigquery?project=farescout-506518

# Query from Python
.venv/Scripts/python.exe query_baselines.py
.venv/Scripts/python.exe query_deals.py
```

### dbt
```bash
cd dbt

# Run all models
../.venv/Scripts/dbt.exe run

# Run specific model
../.venv/Scripts/dbt.exe run --select mart_deal_scores

# Run tests
../.venv/Scripts/dbt.exe test

# Generate docs
../.venv/Scripts/dbt.exe docs generate
../.venv/Scripts/dbt.exe docs serve
```

### Data Generation
```bash
# Load historical data (one quarter)
.venv/Scripts/python.exe -m src.ingestion.dot_fares --year 2025 --quarters 3

# Generate synthetic snapshots
.venv/Scripts/python.exe -m src.ingestion.price_simulator --routes 10 --days-ahead 30
```

## Portfolio Presentation Tips

**What to Highlight:**
1. **Data Modeling** - Show dbt lineage graph, explain staging → intermediate → marts
2. **BigQuery Optimization** - Partitioning, clustering, incremental processing
3. **Data Quality** - dbt tests, data validation, freshness checks
4. **Orchestration** - Airflow DAGs with dependencies, retries, failure handling
5. **Event-Driven Design** - Decoupled architecture, queue-based processing
6. **End-to-End Ownership** - Ingestion → Transform → Serve → Query

**Screenshots to Take:**
- dbt lineage graph
- Airflow DAG graph view
- BigQuery table schema with partitioning
- Query results showing deal scores
- Test results (all passing)

## Known Issues

1. **Price simulator generates prices too high** (7-8x baseline)
   - Fix multipliers in line ~80 of `src/ingestion/price_simulator.py`

2. **No Q3/Q4 2025 baseline data**
   - DOT hasn't released yet
   - Current snapshots use Q2 departures to match available baselines

3. **BigQuery free tier limits**
   - 10 GB storage (currently ~2-3 GB used)
   - 1 TB queries/month
   - DML requires billing (workaround: WRITE_TRUNCATE instead of DELETE)

## Resources

- BigQuery Console: https://console.cloud.google.com/bigquery?project=farescout-506518
- dbt Docs: https://docs.getdbt.com/
- Airflow Docs: https://airflow.apache.org/docs/
- GitHub Repo: https://github.com/samuelcooper-dev/FareScout

## When You Come Back

**Quick start:**
1. Check BigQuery storage usage (stay under 10 GB)
2. Run `query_deals.py` to see current deal scores
3. Pick a phase from above (recommend Phase 3: Airflow)
4. Update this file with progress
5. Commit and push changes regularly

**Last session ended:** 2026-08-24 (Phase 2 complete)
