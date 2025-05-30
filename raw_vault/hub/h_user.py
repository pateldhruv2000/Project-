import pandas as pd
import hashlib
import os
from datetime import datetime

# Helper function to create a SHA-256 hash
def generate_hash_key(row, keys):
    concat_str = '||'.join([str(row[k]) for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# Load CSV
user_df = pd.read_csv("/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user.csv")

# Business key columns
business_keys = ["user_id"]

# Generate hash key for each row
user_df['hash_key'] = user_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Add metadata
user_df['load_date'] = datetime.now().isoformat()
user_df['record_source'] = 'hub_user'

# Select only necessary columns
hub_user_df = user_df[['hash_key'] + business_keys + ['load_date', 'record_source']]

# Save to output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hubs/"
os.makedirs(output_dir, exist_ok=True)
hub_user_df.to_parquet(os.path.join(output_dir, "hub_user.parquet"), index=False)

print("✅ Hub User created successfully (Pandas).")
