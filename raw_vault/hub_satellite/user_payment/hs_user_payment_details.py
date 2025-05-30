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
user_payment_detail_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_payment_detail.csv"

# Output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_payment_detail/"
hub_filename = "hub_user_payment_detail.parquet"
satellite_filename = "sat_user_payment_detail_details.parquet"

# ---- Load Data ----
# Load user_payment_detail CSV
user_payment_df = pd.read_csv(user_payment_detail_path)

# Step 2: Clean Column Names
def clean_columns(df):
    return df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('-', '_'))

user_payment_df = clean_columns(user_payment_df)

# Step 3: Generate Hash Key
business_keys = ["payment_detail_id"]
user_payment_df['hash_key'] = user_payment_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 4: Add Metadata
load_time = datetime.now().isoformat()
user_payment_df['load_date'] = load_time
user_payment_df['record_source'] = 'user_payment_detail_source'

# Step 5: Hub Table
hub_columns = ['hash_key'] + business_keys + ['load_date', 'record_source']
hub_df = user_payment_df[hub_columns]

# Step 6: Satellite Table
satellite_columns = [col for col in user_payment_df.columns if col not in business_keys and col != 'hash_key']
satellite_df = user_payment_df[['hash_key'] + satellite_columns]

# Step 7: Save Outputs
os.makedirs(output_dir, exist_ok=True)
hub_df.to_parquet(os.path.join(output_dir, hub_filename), index=False)
satellite_df.to_parquet(os.path.join(output_dir, satellite_filename), index=False)

print(f"✅ Hub and Satellite for User Payment Detail created successfully!\nHub: {hub_filename}\nSatellite: {satellite_filename}")
