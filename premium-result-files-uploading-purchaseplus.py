from google.cloud import bigquery
import pandas as pd
import numpy as np
import os
import sys
import time
from datetime import date, datetime
import logging
import pandas_gbq


daily_pp_yyyymm = "202111"  # Input by user
hotel_group = "IHG"   # Input by user

# ========================================= Connect to BigQuery ========================================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"
table_name_premium_price = "calculated_premium_price"


# Instantiates a client
client_bq = bigquery.Client()
dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
table_ref_remium_price = dataset_ref_purchaseplus_dashboard.table(table_name_premium_price)

# Get table schema
table_premium_price = client_bq.get_table(table_ref_remium_price)
print(table_premium_price.schema)

# Loop to create schema
table_premium_price_schema = [{'name': schema.name, 'type': schema.field_type} for schema in table_premium_price.schema]
print(table_premium_price_schema)

# Big Query log
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# ======================================================================================================================
# Todo: Read folder name in daily_pp_yyyymm to create purchaser_hotels_List
premium_result_folder_path = "D:\\P+ Dashboard\\Data\\Premium_Result\\"+daily_pp_yyyymm
premium_result_folder_list = os.listdir(premium_result_folder_path)
print(premium_result_folder_list)

# Use if some file.xlsx can't upload for some reason
# premium_result_folder_list = ['Amburaya Resort Co Ltd (Branch No 00001) Tax ID _ 0105545051910_Premium_Result_202108.xlsx']

for premium_result_file_name in premium_result_folder_list:
    print(premium_result_file_name)
    premium_result_file_path = 'D:\\P+ Dashboard\\Data\\Premium_Result\\'+daily_pp_yyyymm+"\\"+premium_result_file_name
    premium_result_df = pd.read_excel(premium_result_file_path, engine='openpyxl')

    # Process premium_result_df
    premium_result_df['Document_Date'] = premium_result_df['Document_Date'].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
    print(premium_result_df['Document_Date'])

    premium_result_df['Report_Month'] = premium_result_df['Report_Month'].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
    print(premium_result_df['Report_Month'])

    premium_result_df['Date_Upload'] = premium_result_df['Date_Upload'].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
    print(premium_result_df['Date_Upload'])

    # Convert NaT to None in premium_result_df['Date_Price_Daily']
    premium_result_df['Date_Price_Daily'] = premium_result_df['Date_Price_Daily'].astype(str)
    premium_result_df['Date_Price_Daily'] = premium_result_df['Date_Price_Daily'].apply(lambda x: None if x == "NaT" or "nan" else x)
    print(premium_result_df['Date_Price_Daily'])

    try:
        premium_result_df['Date_Price_Daily'] = premium_result_df['Date_Price_Daily'].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
        print(premium_result_df['Date_Price_Daily'])
    except ValueError:
        pass

    # Todo: Writing code that append pandas dataframe into Google
    print("Start loading premium_result_df to BigQuery table")
    premium_price_table_bq = 'Purchaseplus_Dashboard.calculated_premium_price'
    start = time.time()
    pandas_gbq.to_gbq(premium_result_df, premium_price_table_bq, project_id=project_id, chunksize=2000, \
                      if_exists='append', table_schema=table_premium_price_schema)
    end = time.time()
    print("time alternative 1 " + str(end - start))