import pandas as pd
import hashlib
import os
from datetime import datetime

# Helper function to generate SHA-256 hash key
def generate_hash_key(row, keys):
    """Generate a hash key based on multiple keys."""
    concat_str = '||'.join([str(row[k]).strip() for k in keys])
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

# ---- Configurable Section ----
# Absolute Paths
user_registration_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_registration.csv"
user_plan_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/user_plan.csv"

# Output
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/links/user_registration_plan/"
link_filename = "link_user_registration_plan.parquet"

# ---- Load Data ----
# Load both tables
user_registration_df = pd.read_csv(user_registration_path)
user_plan_df = pd.read_csv(user_plan_path)

# Step 2: Clean Column Names
def clean_columns(df):
    return df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('-', '_'))

user_registration_df = clean_columns(user_registration_df)
user_plan_df = clean_columns(user_plan_df)

# Step 3: Select only the necessary keys
# From user_registration: user_registration_id
# From user_plan: user_registration_id, plan_id
<<<<<<< HEAD
user_registration_keys = user_registration_df[['user_id','user_registration_id']]
user_plan_keys = user_plan_df[['user_registration_id', 'payment_detail_id']]
=======
user_registration_keys = user_registration_df[['user_registration_id']]
user_plan_keys = user_plan_df[['user_registration_id', 'plan_id']]
>>>>>>> c54ee89041345f8d62283b890c5e33b69343fe51

# Step 4: Join on user_registration_id
link_df = pd.merge(user_plan_keys, user_registration_keys, on='user_registration_id', how='inner')

# Step 5: Generate Link Hash Key
<<<<<<< HEAD
business_keys = ["user_id", "payment_detail_id"]
=======
business_keys = ["user_registration_id", "plan_id"]
>>>>>>> c54ee89041345f8d62283b890c5e33b69343fe51
link_df['link_hash_key'] = link_df.apply(lambda row: generate_hash_key(row, business_keys), axis=1)

# Step 6: Add Metadata
load_time = datetime.now().isoformat()
link_df['load_date'] = load_time
link_df['record_source'] = 'user_registration_plan_link'

# Step 7: Organize columns
link_final_df = link_df[['link_hash_key'] + business_keys + ['load_date', 'record_source']]
<<<<<<< HEAD
print(link_final_df.columns)
=======

>>>>>>> c54ee89041345f8d62283b890c5e33b69343fe51
# Step 8: Save Output
os.makedirs(output_dir, exist_ok=True)
link_final_df.to_parquet(os.path.join(output_dir, link_filename), index=False)

print(f"✅ Link Table for User Registration - Plan created successfully!\nLink: {link_filename}")
