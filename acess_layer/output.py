import duckdb

# Paths to your parquet files
play_session_path = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_play_session/sat_user_play_session_details.parquet"
user_plan_path = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_plan/sat_user_plan_details.parquet"
plan_ref_path = (
    "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/ref_plan_payment_frequency.parquet"
)

# Connect to DuckDB (in-memory DB)
con = duckdb.connect(database=":memory:")

# Register Parquet files as DuckDB tables
con.execute(f"CREATE TABLE play_session AS SELECT * FROM read_parquet('{play_session_path}')")
con.execute(f"CREATE TABLE user_plan AS SELECT * FROM read_parquet('{user_plan_path}')")
con.execute(f"CREATE TABLE plan_ref AS SELECT * FROM read_parquet('{plan_ref_path}')")

# -------------------------------
# Insight 1: Play Sessions Online vs Mobile App
# -------------------------------
query1 = """
# SELECT channel_description, COUNT(*) AS session_count
# FROM play_session
# GROUP BY channel_description
# ORDER BY session_count DESC
"""

result1 = con.execute(query1).fetchdf()

# -------------------------------
# Insight 2: Registered Users One-Time vs Subscription
# -------------------------------
query2 = """
# SELECT 
#     CASE 
#         WHEN payment_frequency_code = 'O' THEN 'One-Time Payment'
#         ELSE 'Subscription'
#     END AS payment_type,
#     COUNT(*) AS user_count
# FROM user_plan
# LEFT JOIN plan_ref USING(plan_id)
# GROUP BY payment_type
"""

result2 = con.execute(query2).fetchdf()

# -------------------------------
# Insight 3: Gross Revenue Generated
# -------------------------------
query3 = """
# SELECT SUM(cost_amount) AS total_revenue
# FROM user_plan
# LEFT JOIN plan_ref USING(plan_id)
"""

result3 = con.execute(query3).fetchdf()

# -------------------------------
# Final Output
# -------------------------------
print("\n🎯 Business Insights for 2025 (SQL)")
print("----------------------------------")

# Insight 1
print("\n1️⃣ Play Sessions by Channel (Web vs Mobile):")
print(result1)

# Insight 2
print("\n2️⃣ Registered Users by Payment Type (One-Time vs Subscription):")
print(result2)

# Insight 3
print("\n3️⃣ Gross Revenue Generated from the App:")
print(f"💰 Total Revenue: ${result3['total_revenue'][0]:,.2f}")
####need to work on output Qury file it is not correct should be from hub & hub_satellite
