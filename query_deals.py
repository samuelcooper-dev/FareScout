from google.cloud import bigquery
from src.config import GCP_PROJECT_ID

client = bigquery.Client(project=GCP_PROJECT_ID)

query = """
SELECT
  route_origin,
  route_dest,
  carrier,
  departure_date,
  days_to_departure,
  price_usd,
  baseline_median_usd,
  deal_score,
  is_deal
FROM `farescout-506518.raw_analytics.mart_deal_scores`
WHERE baseline_median_usd IS NOT NULL
ORDER BY deal_score DESC
LIMIT 20
"""

print("Top 20 Flight Deals (by deal score):\n")
print(f"{'Route':<12} {'Carrier':<7} {'Departs':<12} {'Days Out':<9} {'Price':<8} {'Baseline':<9} {'Score':<7} {'Deal?'}")
print("-" * 90)

for row in client.query(query).result():
    deal_flag = "✓ DEAL" if row.is_deal else ""
    print(f"{row.route_origin}-{row.route_dest:<7} {row.carrier:<7} {str(row.departure_date):<12} {row.days_to_departure:<9} ${row.price_usd:<7.0f} ${row.baseline_median_usd:<8.0f} {row.deal_score:>6.1%} {deal_flag}")
