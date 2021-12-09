from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import date
import logging
import pandas_gbq

hotel_group = "IHG"
fnf_category_file_name = "Food&Non-food category.csv"
fnf_category_file_path = "D:\\Data\\"+hotel_group+"\\"+fnf_category_file_name
print(fnf_category_file_path)
fnf_category_df = pd.read_csv(fnf_category_file_path)

# Replace white space in column name of pandas dataframe to "_" (In order to make column name match with BigQuery Table)
fnf_category_df.columns = fnf_category_df.columns.str.replace(" ", "_")
pd_column_list = fnf_category_df.columns.values.tolist()
print(pd_column_list)

# Todo: check which food and non-food category already uploaded to BigQuery \
# and upload new category that are not existing yet?

print("Finish process fnf_category File")

# =====================================# Connect to BigQuery schema and upload to BigQuery =============================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"
table_name_fnf_category = "food_non-food_category"

# Instantiates a client
client_bq = bigquery.Client()
dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
table_ref_fnf_category = dataset_ref_purchaseplus_dashboard.table(table_name_fnf_category)

# Get table schema
table_fnf_category = client_bq.get_table(table_ref_fnf_category)
print(table_fnf_category.schema)

# Loop to create schema
table_fnf_category_schema = [{'name':schema.name, 'type':schema.field_type} for schema in table_fnf_category.schema]
print(table_fnf_category_schema)

# Big Query log
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Todo: Writing code that append pandas dataframe into Google
print("Start loading fnf_category_df to BigQuery table")
fnf_category_table_bq = 'Purchaseplus_Dashboard.food_non-food_category'
start = time.time()
pandas_gbq.to_gbq(fnf_category_df, fnf_category_table_bq, project_id=project_id, chunksize=2000,\
                  if_exists='append', table_schema=table_fnf_category_schema)
end = time.time()
print("time alternative 1 " + str(end - start))