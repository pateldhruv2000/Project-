import pandas as pd
import os

# Folder where CSV files are placed
data_dir = "/home/sagemaker-user/dice_model_project/Project-/data/"
output_dir = "/home/sagemaker-user/dice_model_project/Project-/output/staged_csvs/"
os.makedirs(output_dir, exist_ok=True)

# List of expected CSV filenames (without extension)
table_names = [
    "user",
    "user_registration",
    "user_play_session",
    "user_plan",
    "user_payment_detail",
    "channel_code",
    "status_code",
    "plan",
    "plan_payment_frequency"
]

# Process each CSV file and re-save (stage)
for table in table_names:
    csv_path = os.path.join(data_dir, f"{table}.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)  # <-- Changed this line
        df.to_csv(os.path.join(output_dir, f"{table}.csv"), index=False)
        print(f"✔️ Staged {table}.csv")
    else:
        print(f"⚠️ {table}.csv not found in {data_dir}")
