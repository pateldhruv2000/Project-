import pandas as pd
import os

# ---- Config ----
# Example: Point to some hub, satellite, link files you want to check
checks = {
    "hub_user": "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user/hub_user.parquet",
    "sat_user_details": "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hub_satellite/user/sat_user_details.parquet",
    "link_user_play_session": "/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/links/user_play_session/link_user_play_session.parquet"
}

# Business Key Columns to Check for each entity
business_keys = {
    "hub_user": ["user_id"],
    "sat_user_details": ["hash_key"],
    "link_user_play_session": ["user_id", "play_session_id"]
}

# ---- Data Quality Tests ----
def test_no_nulls(df, key_columns):
    """Test if key columns have nulls."""
    nulls = df[key_columns].isnull().sum()
    return nulls.sum() == 0

def test_no_duplicates(df, key_columns):
    """Test if business keys are unique (no duplicate)."""
    duplicates = df.duplicated(subset=key_columns).sum()
    return duplicates == 0

def test_file_exists(filepath):
    """Test if file exists."""
    return os.path.exists(filepath)

# ---- Run Tests ----
results = []

for name, path in checks.items():
    print(f"🔎 Running Tests for: {name}")
    
    if not test_file_exists(path):
        results.append((name, "❌ File does not exist"))
        continue
    
    df = pd.read_parquet(path)

    # Test 1: No Null Business Keys
    if test_no_nulls(df, business_keys[name]):
        results.append((name, "✅ No NULLs in business keys"))
    else:
        results.append((name, "❌ NULLs found in business keys"))

    # Test 2: No Duplicate Business Keys
    if test_no_duplicates(df, business_keys[name]):
        results.append((name, "✅ No Duplicates in business keys"))
    else:
        results.append((name, "❌ Duplicates found in business keys"))

# ---- Show Results ----
print("\n🧪 Data Quality Test Results:")
for r in results:
    print(f"{r[0]}: {r[1]}")
