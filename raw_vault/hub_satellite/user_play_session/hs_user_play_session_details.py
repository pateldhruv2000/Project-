import pandas as pd
import os
from datetime import datetime
import hashlib

# Helper function to create SHA-256 hash
def generate_hash_key(row, keys):
    concat_str = '||'.join([str(row[k]).strip() for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# ---- Configurable Section ----
# Absolute Paths
play_session_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_play_session.csv"

# Reference tables (already Parquet)
channel_ref_path = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/ref_play_session_channel_code.parquet"
status_ref_path = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/ref_play_session_status_code.parquet"

# Output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user_play_session/"
hub_filename = "hub_user_play_session.parquet"
satellite_filename = "sat_user_play_session_details.parquet"

# ---- Load Data ----
# Load play session CSV
play_session_df = pd.read_csv(play_session_path)

# Load reference Parquet files
channel_df = pd.read_parquet(channel_ref_path)
status_df = pd.read_parquet(status_ref_path)

# Step 2: Clean Column Names
def clean_columns(df):
    return df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('-', '_'))

play_session_df = clean_columns(play_session_df)
channel_df = clean_columns(channel_df)
status_df = clean_columns(status_df)

# Step 3: Prepare Reference Tables
channel_df = channel_df.rename(columns={'english_description': 'channel_description'})
status_df = status_df.rename(columns={'english_description': 'status_description'})

# Step 4: Join Play Session with Reference Tables
play_session_df = play_session_df.merge(
    channel_df[['play_session_channel_code', 'channel_description']],
    how='left',
    left_on='channel_code',
    right_on='play_session_channel_code'
)

play_session_df = play_session_df.merge(
    status_df[['play_session_status_code', 'status_description']],
    how='left',
    left_on='status_code',
    right_on='play_session_status_code'
)

# Step 5: Generate Hash Key
business_keys = ["play_session_id"]
play_session_df['hash_key'] = play_session_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 6: Add Metadata
load_time = datetime.now().isoformat()
play_session_df['load_date'] = load_time
play_session_df['record_source'] = 'user_play_session_source'
play_session_df['start_date'] = load_time
play_session_df['end_date'] = None

# Step 7: Create Hub
hub_columns = ['hash_key'] + business_keys + ['load_date', 'record_source', 'start_date', 'end_date']
hub_df = play_session_df[hub_columns]

# Step 8: Create Satellite

satellite_columns = [col for col in play_session_df.columns if col not in business_keys and col != 'hash_key']
satellite_df = play_session_df[['hash_key'] + satellite_columns]


# Step 9: Save Output
os.makedirs(output_dir, exist_ok=True)
hub_df.to_parquet(os.path.join(output_dir, hub_filename), index=False)
satellite_df.to_parquet(os.path.join(output_dir, satellite_filename), index=False)

print(f"✅ Hub and Satellite for User Play Session created successfully!\nHub: {hub_filename}\nSatellite: {satellite_filename}")
