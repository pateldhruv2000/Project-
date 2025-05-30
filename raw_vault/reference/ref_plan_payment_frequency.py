import pandas as pd
import os

# ---- Configurable Section ----
# Absolute paths
payment_freq_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/plan_payment_frequency.csv"
plan_path = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/plan.csv"
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/reference/"

# Output file name
combined_ref_filename = "ref_plan_payment_frequency.parquet"

# ---- Load and Join Tables ----
# Step 1: Load Tables
payment_df = pd.read_csv(payment_freq_path)
plan_df = pd.read_csv(plan_path)

# Step 2: Clean Column Names (important for Parquet)
payment_df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in payment_df.columns]
plan_df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in plan_df.columns]

# Step 3: Merge Tables
# Join on payment_frequency_code
combined_df = plan_df.merge(payment_df, on='payment_frequency_code', how='left')

# Step 4: Save to Parquet
os.makedirs(output_dir, exist_ok=True)
combined_df.to_parquet(os.path.join(output_dir, combined_ref_filename), index=False)

print(f"✅ Combined Reference Table for Plan and Payment Frequency created successfully!\nReference File: {combined_ref_filename}")
