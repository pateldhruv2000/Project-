import pandas as pd
import os
from datetime import datetime
import hashlib

# Helper function to generate SHA-256 hash
def generate_hash_key(row, keys):
    concat_str = '||'.join([str(row[k]).strip() for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# ---- Configurable Section ----
# Absolute Paths
user_plan_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_plan.csv"

# Reference table (already Parquet)
plan_ref_path = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/ref_plan_payment_frequency.parquet"

# Output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_plan/"
hub_filename = "hub_user_plan.parquet"
satellite_filename = "sat_user_plan_details.parquet"

# ---- Load Data ----
# Load user plan CSV
user_plan_df = pd.read_csv(user_plan_path)

# Load reference plan Parquet
plan_ref_df = pd.read_parquet(plan_ref_path)

# Step 2: Clean Column Names
def clean_columns(df):
    return df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('-', '_'))

user_plan_df = clean_columns(user_plan_df)
plan_ref_df = clean_columns(plan_ref_df)

# Step 3: Join with Reference Plan Table
user_plan_df = user_plan_df.merge(
    plan_ref_df[['plan_id', 'payment_frequency_code', 'cost_amount', 'english_description', 'french_description']],
    how='left',
    on='plan_id'
)

# Step 4: Generate Hash Key
<<<<<<< HEAD
business_keys = ["payment_detail_id"]
=======
business_keys = ["user_registration_id", "payment_detail_id", "plan_id"]
>>>>>>> c54ee89041345f8d62283b890c5e33b69343fe51
user_plan_df['hash_key'] = user_plan_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 5: Add Metadata
load_time = datetime.now().isoformat()
user_plan_df['load_date'] = load_time
user_plan_df['record_source'] = 'user_plan_source'

# Step 6: Hub Table
hub_columns = ['hash_key'] + business_keys + ['load_date', 'record_source']
hub_df = user_plan_df[hub_columns]

# Step 7: Satellite Table
<<<<<<< HEAD
satellite_columns = [col for col in user_plan_df.columns if col not in business_keys and col != 'hash_key']
satellite_df = user_plan_df[['hash_key'] + satellite_columns]
print(satellite_df.columns)
=======
satellite_columns = [col for col in user_plan_df.columns if col not in business_keys]
satellite_df = user_plan_df[['hash_key'] + satellite_columns]

>>>>>>> c54ee89041345f8d62283b890c5e33b69343fe51
# Step 8: Save Outputs
os.makedirs(output_dir, exist_ok=True)
hub_df.to_parquet(os.path.join(output_dir, hub_filename), index=False)
satellite_df.to_parquet(os.path.join(output_dir, satellite_filename), index=False)

print(f"✅ Hub and Satellite for User Plan created successfully!\nHub: {hub_filename}\nSatellite: {satellite_filename}")
