import pandas as pd
import hashlib
import os
from datetime import datetime

# Helper function to generate SHA-256 hash
def generate_hash_key(row, keys):
    concat_str = '||'.join([str(row[k]).strip() for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# ---- Configurable Section ----
# Absolute paths
input_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_registration.csv"
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_registration/"

# Business Key
business_keys = ["user_registration_id"]

# Output file names
hub_filename = "hub_user_registration.parquet"
satellite_filename = "sat_user_registration_details.parquet"

# ---- Hub & Satellite Creation ----
# Step 1: Load Data
df = pd.read_csv(input_path)

# Step 2: Clean Column Names
df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]

# Step 3: Drop existing system columns if they exist
reserved_columns = ['hash_key', 'load_date', 'record_source']
df = df[[col for col in df.columns if col not in reserved_columns]]

# Step 4: Generate Hash Key
df['hash_key'] = df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 5: Add Metadata
load_time = datetime.now().isoformat()
df['load_date'] = load_time
df['record_source'] = 'user_registration_source'

# Step 6: Hub Table
hub_columns = ['hash_key'] + business_keys + ['load_date', 'record_source']
hub_df = df[hub_columns].drop_duplicates()

# Step 7: Satellite Table
satellite_columns = [col for col in df.columns if col not in business_keys]
satellite_df = df[['hash_key'] + satellite_columns].drop_duplicates()

# Step 8: Save Outputs
os.makedirs(output_dir, exist_ok=True)
hub_df.to_parquet(os.path.join(output_dir, hub_filename), index=False)
satellite_df.to_parquet(os.path.join(output_dir, satellite_filename), index=False)

print(f"✅ Hub and Satellite for User Registration created successfully!\nHub: {hub_filename}\nSatellite: {satellite_filename}")
