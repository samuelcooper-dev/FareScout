"""
Synthetic price snapshot generator.

Generates realistic flight price variations based on historical baselines,
incorporating seasonality, booking windows, and day-of-week patterns.

Usage:
    python -m src.ingestion.price_simulator --routes 10 --days-ahead 30
"""

import argparse
import logging
import random
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from src.config import BQ_DATASET_RAW, GCP_PROJECT_ID

logger = logging.getLogger(__name__)

_BQ_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.price_snapshots"

_BQ_SCHEMA = [
    bigquery.SchemaField("snapshot_id", "STRING"),
    bigquery.SchemaField("polled_at", "TIMESTAMP"),
    bigquery.SchemaField("origin", "STRING"),
    bigquery.SchemaField("dest", "STRING"),
    bigquery.SchemaField("carrier", "STRING"),
    bigquery.SchemaField("cabin_class", "STRING"),
    bigquery.SchemaField("price_usd", "FLOAT"),
    bigquery.SchemaField("seats_remaining", "INTEGER"),
    bigquery.SchemaField("departure_date", "DATE"),
    bigquery.SchemaField("days_to_departure", "INTEGER"),
]


def get_top_routes(client: bigquery.Client, limit: int = 10) -> pd.DataFrame:
    """Fetch top routes by passenger volume from baselines."""
    query = f"""
    SELECT
        route_origin,
        route_dest,
        ANY_VALUE(median_fare_usd) as median_fare_usd,
        SUM(total_passengers) as total_passengers
    FROM `{GCP_PROJECT_ID}.raw_intermediate.int_route_baselines`
    WHERE sample_size > 100
    GROUP BY route_origin, route_dest
    ORDER BY total_passengers DESC
    LIMIT {limit}
    """
    return client.query(query).to_dataframe()


def generate_snapshots(
    routes: pd.DataFrame,
    days_ahead: int = 30,
    carriers: list = None,
) -> pd.DataFrame:
    """Generate synthetic price snapshots for given routes."""
    if carriers is None:
        carriers = ["AA", "DL", "UA", "WN", "B6"]

    snapshots = []
    polled_at = pd.Timestamp.now("UTC")

    for _, route in routes.iterrows():
        baseline = float(route["median_fare_usd"])

        # Generate snapshots for Q2 2025 (Apr-Jun) to match baseline data
        # Use dates in May 2025 for demonstration
        base_departure = datetime(2025, 5, 1).date()

        # Generate snapshots across May
        for day_offset in range(1, 30, 3):  # Every 3 days in May
            departure_date = base_departure + timedelta(days=day_offset)
            days_out = (departure_date - datetime.now().date()).days

            for carrier in random.sample(carriers, k=2):  # 2 carriers per route
                # Price variation factors
                booking_window_factor = 1.0 + (0.02 * max(0, 30 - days_out))  # Rises as departure nears
                day_of_week = departure_date.weekday()
                weekend_factor = 1.15 if day_of_week in [4, 5] else 1.0  # Friday/Saturday higher
                random_variance = random.uniform(0.85, 1.15)  # ±15% noise

                price = baseline * booking_window_factor * weekend_factor * random_variance

                snapshots.append({
                    "snapshot_id": f"{route['route_origin']}-{route['route_dest']}-{carrier}-{departure_date}-{polled_at.isoformat()}",
                    "polled_at": polled_at,
                    "origin": route["route_origin"],
                    "dest": route["route_dest"],
                    "carrier": carrier,
                    "cabin_class": "economy",
                    "price_usd": round(price, 2),
                    "seats_remaining": random.randint(3, 45),
                    "departure_date": departure_date,
                    "days_to_departure": days_out,
                })

    return pd.DataFrame(snapshots)


def ensure_table(client: bigquery.Client) -> None:
    """Create price_snapshots table if it doesn't exist."""
    dataset_ref = client.dataset(BQ_DATASET_RAW)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)
        logger.info("Created dataset %s", BQ_DATASET_RAW)

    table_ref = client.dataset(BQ_DATASET_RAW).table("price_snapshots")
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=_BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="polled_at",
        )
        client.create_table(table)
        logger.info("Created table %s", _BQ_TABLE)


def load_to_bq(df: pd.DataFrame, client: bigquery.Client, truncate: bool = False) -> int:
    """Load snapshots to BigQuery."""
    write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE if truncate else bigquery.WriteDisposition.WRITE_APPEND
    job_config = bigquery.LoadJobConfig(
        schema=_BQ_SCHEMA,
        write_disposition=write_mode,
    )
    job = client.load_table_from_dataframe(df, _BQ_TABLE, job_config=job_config)
    job.result()
    return len(df)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Generate synthetic flight price snapshots")
    parser.add_argument("--routes", type=int, default=10, help="Number of top routes to simulate")
    parser.add_argument("--days-ahead", type=int, default=30, help="Generate prices for next N days")
    args = parser.parse_args()

    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_table(client)

    logger.info("Fetching top %d routes from baselines...", args.routes)
    routes = get_top_routes(client, limit=args.routes)
    logger.info("Found %d routes", len(routes))

    logger.info("Generating snapshots for %d days ahead...", args.days_ahead)
    snapshots = generate_snapshots(routes, days_ahead=args.days_ahead)
    logger.info("Generated %d snapshots", len(snapshots))

    logger.info("Loading to BigQuery...")
    rows = load_to_bq(snapshots, client, truncate=True)
    logger.info("Loaded %d rows to %s", rows, _BQ_TABLE)


if __name__ == "__main__":
    main()
