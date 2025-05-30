import os
import pandas as pd

output_folder = '/home/sagemaker-user/dice_model_project/output/'
download_base_path = '/home/sagemaker-user/dice_model_project/exports/'

h_user = pd.read_parquet('/home/sagemaker-user/dice_model_project/Project-/output/raw_vault/hubs/hub_user.parquet')

h_user.show()