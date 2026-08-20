"""
DOT DB1B fare ingestion.

Downloads quarterly DB1B_Market ZIP files from the BTS TranStats portal,
parses the fare data, and loads it into BigQuery raw.fares_historical.

Usage:
    python -m src.ingestion.dot_fares --year 2023 --quarters 1 2 3 4
    python -m src.ingestion.dot_fares --year 2022  # all 4 quarters
"""

import argparse
import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import BQ_DATASET_RAW, GCP_PROJECT_ID

logger = logging.getLogger(__name__)

# BTS download URL pattern for DB1B Market quarterly files
_BTS_URL = (
    "https://www.transtats.bts.gov/PREZIP/"
    "Origin_and_Destination_Survey_DB1BMarket_{year}_{quarter}.zip"
)

# Only the columns we actually use — keeps memory small on multi-year loads
_KEEP_COLS = [
    "Year",
    "Quarter",
    "Origin",
    "Dest",
    "AirlineID",
    "UniqueCarrier",
    "MktFare",
    "Passengers",
    "Distance",
]

# BigQuery destination
_BQ_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.fares_historical"

_BQ_SCHEMA = [
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("quarter", "INTEGER"),
    bigquery.SchemaField("origin", "STRING"),
    bigquery.SchemaField("dest", "STRING"),
    bigquery.SchemaField("airline_id", "INTEGER"),
    bigquery.SchemaField("carrier", "STRING"),
    bigquery.SchemaField("mkt_fare_usd", "FLOAT"),
    bigquery.SchemaField("passengers", "INTEGER"),
    bigquery.SchemaField("distance_miles", "INTEGER"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def _download_zip(year: int, quarter: int) -> bytes:
    url = _BTS_URL.format(year=year, quarter=quarter)
    logger.info("Downloading %s", url)
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    return resp.content


def _parse_zip(raw_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        # The ZIP contains one CSV; name varies slightly by quarter
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        logger.info("Parsing %s", csv_name)
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, usecols=_KEEP_COLS, low_memory=False)

    # Normalise
    df = df.rename(
        columns={
            "Year": "year",
            "Quarter": "quarter",
            "Origin": "origin",
            "Dest": "dest",
            "AirlineID": "airline_id",
            "UniqueCarrier": "carrier",
            "MktFare": "mkt_fare_usd",
            "Passengers": "passengers",
            "Distance": "distance_miles",
        }
    )

    # Drop rows with no fare or no passengers (null/zero values are noise)
    df = df[(df["mkt_fare_usd"] > 0) & (df["passengers"] > 0)].copy()

    df["loaded_at"] = pd.Timestamp.utcnow()
    return df


def _load_to_bq(df: pd.DataFrame, client: bigquery.Client) -> int:
    job_config = bigquery.LoadJobConfig(
        schema=_BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Partition by year+quarter so we can overwrite specific quarters cheaply
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="loaded_at",
        ),
    )
    job = client.load_table_from_dataframe(df, _BQ_TABLE, job_config=job_config)
    job.result()  # blocks until complete
    return len(df)


def load_quarter(year: int, quarter: int, client: bigquery.Client) -> int:
    """Download one quarter, parse it, load to BQ. Returns row count."""
    raw = _download_zip(year, quarter)
    df = _parse_zip(raw)
    rows = _load_to_bq(df, client)
    logger.info("Loaded %d rows for %dQ%d", rows, year, quarter)
    return rows


def ensure_table(client: bigquery.Client) -> None:
    """Create the destination table if it does not exist."""
    dataset_ref = client.dataset(BQ_DATASET_RAW)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)
        logger.info("Created dataset %s", BQ_DATASET_RAW)

    table_ref = client.dataset(BQ_DATASET_RAW).table("fares_historical")
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=_BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="loaded_at",
        )
        client.create_table(table)
        logger.info("Created table %s", _BQ_TABLE)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Load DOT DB1B fare data into BigQuery")
    parser.add_argument("--year", type=int, required=True, help="e.g. 2023")
    parser.add_argument(
        "--quarters",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
        choices=[1, 2, 3, 4],
        help="Quarters to load (default: all four)",
    )
    args = parser.parse_args()

    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_table(client)

    total = 0
    errors = []
    for q in args.quarters:
        try:
            total += load_quarter(args.year, q, client)
        except Exception as exc:
            # Per-quarter failure isolation — one bad quarter doesn't kill the run
            logger.error("Failed %dQ%d: %s", args.year, q, exc)
            errors.append((args.year, q, str(exc)))

    logger.info("Done. Total rows loaded: %d", total)
    if errors:
        logger.warning("Failed quarters: %s", errors)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
