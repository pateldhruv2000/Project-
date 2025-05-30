
import pandas as pd
import os

# ---- Configurable Section ----
# Absolute input and output paths
input_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/channel_code.csv"
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/"

# Output file name
ref_filename = "ref_play_session_channel_code.parquet"

# ---- Load and Save Reference Table ----
# Step 1: Load Data
df = pd.read_csv(input_path)

# Step 2: Clean Column Names (important for Parquet)
df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]

# Step 3: No transformation — Reference tables are stored as-is

# Step 4: Save to Parquet
os.makedirs(output_dir, exist_ok=True)
df.to_parquet(os.path.join(output_dir, ref_filename), index=False)

print(f"✅ Reference Table for Play Session Channel Code created successfully!\nReference File: {ref_filename}")
