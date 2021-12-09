from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import date, datetime
import logging
import pandas_gbq

# pip install openpyxl

po_yyyymm = "202111"  # Input by user
hotel_group = "IHG"  # Input by user

# ======================================================================================================================
po_folder = "Invoice and PO line data"
po_file_name = "IHG PO-"+po_yyyymm+".xlsx"
po_file_path = "D:\\P+ Dashboard\\Data\\"+hotel_group+"\\"+po_folder+"\\"+po_yyyymm+"\\"+po_file_name
print(po_file_path)

# Todo: check if this file name already uploaded to BigQuery?
po_df = pd.read_excel(po_file_path, engine='openpyxl')
# Process po_df
# Remove unused columns name is 'code_1' to 'code_6'
po_df = po_df.drop(['code_1', 'code_2', 'code_3', 'code_4', 'code_5', 'code_6'], axis=1)

# Todo: Check format Date of column "Sent_Date" and "Delivery_Date" sometime have 2-Nov-20 convert to this \
# standard format ==> 2020-11-02
# Covert datetime format in column "Document Date" to only date format without time
po_df['Sent Date'] = po_df['Sent Date'].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') if str(x) != 'NaT' else None)
print(po_df['Sent Date'])
po_df['Delivery Date'] = po_df['Delivery Date'].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') if str(x) != 'NaT' else None)
print(po_df['Delivery Date'])
print(po_df[po_df['Delivery Date'].isnull()])
# Todo: Adding column 'File Name' and 'Date Uplaod' to the right-end of Dataframe
# Adding File Name Column that has constant today date to existing pandas dataframe
upload_date = date.today()
po_df['Hotel_Group'] = hotel_group
po_df['File Name'] = po_file_name
po_df['Date Upload'] = upload_date
report_month_datetimeobject = datetime.strptime(po_yyyymm, '%Y%m')
report_month_newformat = report_month_datetimeobject.strftime('%Y-%m-01')
po_df['Report_Month'] = report_month_newformat

# Replace white space in column name of pandas dataframe to "_" (In order to make column name match with BigQuery Table)
po_df.columns = po_df.columns.str.replace(" ", "_")
pd_column_list = po_df.columns.values.tolist()
print(pd_column_list)
print(po_df)
print("Finish process PO Files")

# Export invoice_df to csv format for create BigQuery schema
# po_df.to_csv('D:\\po_df.csv', index=False, header=True)
# sys.exit()
# =====================================# Connect to BigQuery schema and upload to BigQuery =============================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"
table_name_po = "purchase_order"

# Instantiates a client
client_bq = bigquery.Client()
dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
table_ref_po = dataset_ref_purchaseplus_dashboard.table(table_name_po)

# Get table schema
table_po = client_bq.get_table(table_ref_po)
print(table_po.schema)

# Loop to create schema
table_po_schema = [{'name': schema.name, 'type': schema.field_type} for schema in table_po.schema]
print(table_po_schema)

# Big Query log
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Todo: Writing code that append pandas dataframe into Google
print("Start loading po_df to BigQuery table")
po_table_bq = 'Purchaseplus_Dashboard.purchase_order'
start = time.time()
pandas_gbq.to_gbq(po_df, po_table_bq, project_id=project_id, chunksize=2000,\
                  if_exists='append', table_schema=table_po_schema)
end = time.time()
print("time alternative 1 " + str(end - start))