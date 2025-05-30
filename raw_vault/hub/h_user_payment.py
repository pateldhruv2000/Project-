import pandas as pd
import hashlib
import os
from datetime import datetime

# Helper function to create a SHA-256 hash
def generate_hash_key(row, keys):
    concat_str = '||'.join([str(row[k]) for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# Load CSV
payment_detail_df = pd.read_csv("/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_plan.csv")

# Business key columns
business_keys = ["payment_detail_id"]

# Generate hash key for each row
payment_detail_df['hash_key'] = payment_detail_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Add metadata
payment_detail_df['load_date'] = datetime.now().isoformat()
payment_detail_df['record_source'] = 'hub_user_payment_detail'

# Select only necessary columns
hub_payment_detail_df = payment_detail_df[['hash_key'] + business_keys + ['load_date', 'record_source']]

# Save to output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hubs/"
os.makedirs(output_dir, exist_ok=True)
hub_payment_detail_df.to_parquet(os.path.join(output_dir, "hub_user_payment_detail.parquet"), index=False)

print("✅ Hub User Payment Detail created successfully (Pandas).")
