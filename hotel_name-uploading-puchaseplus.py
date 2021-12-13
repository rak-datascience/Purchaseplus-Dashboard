from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import date
import logging
import pandas_gbq

hotel_group = "IHG"
hotel_name_file_name = "Hotel name.xlsx"
hotel_name_file_path = "D:\\P+ Dashboard\\Data\\General\\"+hotel_name_file_name
print(hotel_name_file_path)
hotel_name_df = pd.read_excel(hotel_name_file_path, engine='openpyxl')
# Replace white space in column name of pandas dataframe to "_" (In order to make column name match with BigQuery Table)
hotel_name_df.columns = hotel_name_df.columns.str.replace(" ", "_")
hotel_name_df = hotel_name_df.loc[:, ~hotel_name_df.columns.str.contains('^Unnamed')]  # Drop Unnamed columns from dataframe
hotel_name_df = hotel_name_df.dropna(how='all')  # Drop entire empty row from dataframe
pd_column_list = hotel_name_df.columns.values.tolist()
print(pd_column_list)
print(hotel_name_df)
print("Finish process hotel_name File")

# =====================================# Connect to BigQuery schema and upload to BigQuery =============================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"
table_hotel_name = "hotel_name"

# Instantiates a client
client_bq = bigquery.Client()
dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
table_ref_hotel_name = dataset_ref_purchaseplus_dashboard.table(table_hotel_name)

# Get table schema
table_hotel_name = client_bq.get_table(table_ref_hotel_name)
print(table_hotel_name.schema)

# Loop to create schema
table_hotel_name_schema = [{'name':schema.name, 'type':schema.field_type} for schema in table_hotel_name.schema]
print('table_hotel_name_schema')
print(table_hotel_name_schema)

# Big Query log
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Todo: Writing code that append pandas dataframe into Google
print("Start loading fnf_category_df to BigQuery table")
hotel_name_table_bq = 'Purchaseplus_Dashboard.hotel_name'
start = time.time()
pandas_gbq.to_gbq(hotel_name_df, hotel_name_table_bq, project_id=project_id, chunksize=2000,\
                  if_exists='append', table_schema=table_hotel_name_schema)
end = time.time()
print("time alternative 1 " + str(end - start))