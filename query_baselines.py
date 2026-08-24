from google.cloud import bigquery
from src.config import GCP_PROJECT_ID

client = bigquery.Client(project=GCP_PROJECT_ID)

query = """
SELECT
  route_origin,
  route_dest,
  quarter_bucket,
  median_fare_usd,
  p25_fare_usd,
  p75_fare_usd,
  sample_size,
  total_passengers
FROM `farescout-506518.raw_intermediate.int_route_baselines`
WHERE sample_size > 100
ORDER BY total_passengers DESC
LIMIT 10
"""

print("Top 10 Routes by Passenger Volume:\n")
for row in client.query(query).result():
    print(f"{row.route_origin}-{row.route_dest} Q{row.quarter_bucket}: ${row.median_fare_usd:.0f} (p25: ${row.p25_fare_usd:.0f}, p75: ${row.p75_fare_usd:.0f}) | {row.sample_size:,} samples, {row.total_passengers:,} passengers")
