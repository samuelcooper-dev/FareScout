"""Central config — loaded from environment / .env file."""
import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


# Amadeus
AMADEUS_CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID", "")
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET", "")
AMADEUS_HOSTNAME = os.getenv("AMADEUS_HOSTNAME", "test")

# BigQuery
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "raw")
BQ_DATASET_ANALYTICS = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

# Queue
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092")
REDPANDA_TOPIC_PRICE_DROP = os.getenv("REDPANDA_TOPIC_PRICE_DROP", "price_drop_events")

# Alert
PRICE_DROP_THRESHOLD = float(os.getenv("PRICE_DROP_THRESHOLD", "0.15"))
