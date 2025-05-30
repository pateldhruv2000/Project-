import pandas as pd
import hashlib
import os
from datetime import datetime

# Helper function to generate SHA-256 hash key
def generate_hash_key(row, keys):
    """Generate a SHA-256 hash key based on multiple business keys."""
    concat_str = '||'.join([str(row[k]).strip() for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# ---- Configurable Section ----
# Absolute Paths
user_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user.csv"
user_play_session_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_play_session.csv"

# Output Path
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/links/user_play_session/"
link_filename = "link_user_play_session.parquet"

# ---- Load Data ----
# Load both tables
user_df = pd.read_csv(user_path)
user_play_session_df = pd.read_csv(user_play_session_path)

# Step 2: Clean Column Names
def clean_columns(df):
    return df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('-', '_'))

user_df = clean_columns(user_df)
user_play_session_df = clean_columns(user_play_session_df)

# Step 3: Select only the necessary keys
# From user: user_id
# From user_play_session: user_id, play_session_id
user_keys = user_df[['user_id']]
user_play_session_keys = user_play_session_df[['user_id', 'play_session_id']]

# Step 4: Join on user_id
link_df = pd.merge(user_play_session_keys, user_keys, on='user_id', how='inner')

# Step 5: Generate Link Hash Key
business_keys = ["user_id", "play_session_id"]
link_df['link_hash_key'] = link_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 6: Add Metadata
load_time = datetime.now().isoformat()
link_df['load_date'] = load_time
link_df['record_source'] = 'user_play_session_link'

# Step 7: Organize Columns
link_final_df = link_df[['link_hash_key'] + business_keys + ['load_date', 'record_source']]

# Step 8: Save Output
os.makedirs(output_dir, exist_ok=True)
link_final_df.to_parquet(os.path.join(output_dir, link_filename), index=False)

print(f"✅ Link Table for User - Play Session created successfully!\nLink: {link_filename}")
